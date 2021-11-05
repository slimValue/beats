// Package pubsuboutput provides a GCP PubSub output for filebeat
package pubsuboutput

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/elastic/beats/v7/libbeat/logp"
	"github.com/elastic/beats/v7/x-pack/elastic-agent/pkg/agent/errors"
	"github.com/google/uuid"
	"strconv"
	"sync"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"google.golang.org/api/option"
)

func init() {
	outputs.RegisterType("pubsub", newPubSubOutput)
}

const logSelector = "pubsub"

type config struct {
	Topic              string          `config:"topic" validate:"required"`
	ProjectID          string          `config:"project_id" validate:"required"`
	CredentialsFile    string          `config:"credentials_file"`
	CredentialsJSON    common.JSONBlob `config:"credentials_json"`
	MaxRetries         int             `config:"max_retries"`
	BulkMaxSize        int             `config:"bulk_max_size"`
	ByteThreshold      int             `config:"byte_threshold"`       // 批量推送 消息大小阀值 默认 1000000
	CountThreshold     int             `config:"count_threshold"`      // 批量推送 消息条数阀值 默认 100
	DelayThreshold     int             `config:"delay_threshold"`      // 批量推送 间隔阀值 默认 1（ms）
	ValidateJsonFields []string        `config:"validate_json_fields"` // 是否校验json字段
}

func defaultConfig() config {
	var c config

	c.BulkMaxSize = 4096

	c.ByteThreshold = 1000000
	c.CountThreshold = 100
	c.DelayThreshold = 1

	return c
}

func newPubSubOutput(_ outputs.IndexManager, info beat.Info, observer outputs.Observer, cfg *common.Config) (outputs.Group, error) {
	c := defaultConfig()
	if err := cfg.Unpack(&c); err != nil {
		return outputs.Fail(err)
	}

	clients := []outputs.NetworkClient{
		&pubsubClient{
			name:     fmt.Sprintf("pubsub:%s/%s", c.ProjectID, c.Topic),
			config:   c,
			observer: observer,
			log:      logp.NewLogger(logSelector),
		},
	}
	return outputs.SuccessNet(false, c.BulkMaxSize, c.MaxRetries, clients)
}

type pubsubClient struct {
	observer outputs.Observer
	client   *pubsub.Client
	topic    *pubsub.Topic
	name     string
	config   config
	log      *logp.Logger
}

func (p *pubsubClient) Connect() error {
	if p.client != nil {
		return nil
	}

	var opts []option.ClientOption

	if p.config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(p.config.CredentialsFile))
	} else if len(p.config.CredentialsJSON) > 0 {
		opts = append(opts, option.WithCredentialsJSON(p.config.CredentialsJSON))
	}

	client, err := pubsub.NewClient(context.Background(), p.config.ProjectID, opts...)
	if err != nil {
		return fmt.Errorf("failed to create pubsub client %w", err)
	}

	p.client = client
	p.topic = client.Topic(p.config.Topic)

	p.topic.PublishSettings.ByteThreshold = p.config.ByteThreshold
	p.topic.PublishSettings.CountThreshold = p.config.CountThreshold
	p.topic.PublishSettings.DelayThreshold = time.Duration(p.config.DelayThreshold) * time.Millisecond

	return nil
}

func (p *pubsubClient) Close() error {
	p.topic.Stop()

	err := p.client.Close()

	p.client = nil

	return err
}

func (p *pubsubClient) String() string {
	return p.name
}

func (p *pubsubClient) Publish(ctx context.Context, batch publisher.Batch) error {
	events := batch.Events()
	p.observer.NewBatch(len(events))

	if len(events) == 0 {
		return nil
	}

	var (
		buf = new(bytes.Buffer)
	)

	// 单条输出至pubsub
	for i := range events {
		e := &events[i]
		/**
		原始消息附带了一些系统信息 如：
		{"Content":{"Timestamp":"2021-10-08T16:20:39.698658047+08:00","Meta":null,"Fields":{"message":"{\"ts\":\"2021-09-23T00:01:07Z\",\"eid\":1632355264682258,\"cid\":1190,\"sid\":190001,\"from_pid\":0,\"time\":1632355264,\"logid\":\"14-900010440-0-1632355267\",\"hid\":0,\"amount2\":0,\"amount\":0,\"account\":\"63521666\",\"subsource\":0,\"pid\":4313181269,\"microtime\":1.632355264682E9,\"ispay\":1,\"type\":1022,\"itemid\":52216,\"source\":22,\"value\":4,\"after\":23650,\"itemdata\":\"\",\"mapid\":127,\"count\":4,\"tag\":\"item\"}"},"Private":{"id":"native::2106335-64769","prev_id":"","source":"/var/log/a.log","offset":831734753,"timestamp":"2021-10-08T16:18:20.318194188+08:00","ttl":-1,"type":"log","meta":null,"FileStateOS":{"inode":2106335,"device":64769},"identifier_name":"native"},"TimeSeries":false},"Flags":1,"Cache":{}}
		现对消息做了处理，只保留input输入的消息主体
		*/
		message, err := e.Content.Fields.GetValue("message")
		if err != nil {
			p.observer.Dropped(len(events))
			batch.Drop()
			return fmt.Errorf("failed to encode event %w", err)
		}

		var msgBytes = interfaceToBytes(message)
		if err = p.validateMessage(msgBytes); err != nil {
			p.observer.Dropped(len(events))
			batch.Drop()
			return fmt.Errorf("failed to encode event %w", err)
		}

		if _, err := buf.Write(msgBytes); err != nil {
			p.observer.Failed(len(events))
			batch.Retry()
			return fmt.Errorf("failed to write message %w", err)
		}

		if _, err := buf.Write([]byte("\n")); err != nil {
			p.observer.Failed(len(events))
			batch.Retry()
			return fmt.Errorf("failed to append newline %w", err)
		}
	}

	msg := pubsub.Message{
		Data: buf.Bytes(),
		Attributes: map[string]string{
			"msgId": uuid.New().String(),
		},
	}
	res := p.topic.Publish(context.Background(), &msg)
	if _, err := res.Get(context.Background()); err != nil {
		p.observer.Failed(len(events))
		batch.Retry()
	}

	batch.ACK()

	return nil
}

func (p *pubsubClient) validateMessage(msgBytes []byte) error {
	if len(p.config.ValidateJsonFields) == 0 {
		return nil
	}

	var msgMap map[string]interface{}
	if err := json.Unmarshal(msgBytes, &msgMap); err != nil {
		return fmt.Errorf("failed to parse json from source event message %w", err)
	}
	if msgMap == nil {
		return errors.New("parse json message is nil")
	}

	for _, field := range p.config.ValidateJsonFields {
		if msgMap[field] == "" {
			return errors.New("parse json message field: " + field + " is nil")
		}
	}
	return nil
}

func interfaceToBytes(v interface{}) []byte {
	switch d := v.(type) {
	case []byte:
		return d
	case string:
		return []byte(d)
	case int, int32, int64, uint, uint32, uint64:
		return []byte(fmt.Sprintf("%d", d))
	case float32, float64:
		return []byte(fmt.Sprintf("%f", d))
	case bool:
		return []byte(strconv.FormatBool(d))
	case time.Time:
		return []byte(d.Format(time.RFC3339))
	default:
		return []byte(fmt.Sprintf("%v", d))
	}
}

var gzipPool = sync.Pool{
	New: func() interface{} {
		return gzip.NewWriter(nil)
	},
}
