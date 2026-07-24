package sink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nvidia/nvsentinel/event-exporter/pkg/transformer"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type KafkaConfig struct {
	Brokers      []string
	Topic        string
	ClientID     string
	RequiredAcks string
	Compression  string
	BatchTimeout time.Duration
	WriteTimeout time.Duration
	TLS          KafkaTLSConfig
	SASL         KafkaSASLConfig
}

type KafkaTLSConfig struct {
	Enabled            bool
	CAFile             string
	CertFile           string
	KeyFile            string
	InsecureSkipVerify bool
}

type KafkaSASLConfig struct {
	Enabled      bool
	Mechanism    string
	Username     string
	PasswordFile string
}

type kafkaProducer interface {
	ProduceSync(context.Context, ...*kgo.Record) kgo.ProduceResults
	Close()
}

type KafkaSink struct {
	topic    string
	producer kafkaProducer
}

func NewKafkaSink(cfg KafkaConfig) (*KafkaSink, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.DefaultProduceTopic(cfg.Topic),
		kgo.RequiredAcks(kafkaAcks(cfg.RequiredAcks)),
		kgo.ProducerBatchCompression(kafkaCompression(cfg.Compression)),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}
	if cfg.BatchTimeout > 0 {
		opts = append(opts, kgo.ProducerLinger(cfg.BatchTimeout))
	}
	if cfg.WriteTimeout > 0 {
		opts = append(opts, kgo.ProduceRequestTimeout(cfg.WriteTimeout))
	}
	if cfg.TLS.Enabled {
		tlsConfig, err := kafkaTLSConfig(cfg.TLS)
		if err != nil {
			return nil, err
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}
	saslOpts, err := kafkaSASLOpts(cfg.SASL)
	if err != nil {
		return nil, err
	}
	opts = append(opts, saslOpts...)

	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}
	return &KafkaSink{topic: cfg.Topic, producer: client}, nil
}

func NewKafkaSinkWithProducer(topic string, producer kafkaProducer) *KafkaSink {
	return &KafkaSink{topic: topic, producer: producer}
}

func (s *KafkaSink) Publish(ctx context.Context, event *transformer.CloudEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	record := &kgo.Record{
		Topic: s.topic,
		Key:   []byte(event.ID),
		Value: body,
		Headers: []kgo.RecordHeader{
			{Key: "content-type", Value: []byte("application/cloudevents+json")},
			{Key: "ce_specversion", Value: []byte(event.SpecVersion)},
			{Key: "ce_type", Value: []byte(event.Type)},
			{Key: "ce_source", Value: []byte(event.Source)},
			{Key: "ce_id", Value: []byte(event.ID)},
		},
	}

	if err := s.producer.ProduceSync(ctx, record).FirstErr(); err != nil {
		return fmt.Errorf("produce kafka record: %w", err)
	}
	return nil
}

func (s *KafkaSink) Close(ctx context.Context) error {
	if s.producer != nil {
		s.producer.Close()
	}
	return nil
}

func kafkaAcks(raw string) kgo.Acks {
	switch raw {
	case "none":
		return kgo.NoAck()
	case "all", "":
		return kgo.AllISRAcks()
	default:
		return kgo.LeaderAck()
	}
}

func kafkaCompression(raw string) kgo.CompressionCodec {
	switch raw {
	case "none":
		return kgo.NoCompression()
	case "gzip":
		return kgo.GzipCompression()
	case "lz4":
		return kgo.Lz4Compression()
	case "zstd":
		return kgo.ZstdCompression()
	default:
		return kgo.SnappyCompression()
	}
}

func kafkaTLSConfig(cfg KafkaTLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12, InsecureSkipVerify: cfg.InsecureSkipVerify} //nolint:gosec // explicitly configurable for test clusters
	if cfg.CAFile != "" {
		caPEM, err := os.ReadFile(cfg.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read kafka ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caPEM) {
			return nil, fmt.Errorf("parse kafka ca_file: no certificates found")
		}
		tlsConfig.RootCAs = pool
	}
	if cfg.CertFile != "" || cfg.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load kafka client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}

func kafkaSASLOpts(cfg KafkaSASLConfig) ([]kgo.Opt, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	passwordBytes, err := os.ReadFile(cfg.PasswordFile)
	if err != nil {
		return nil, fmt.Errorf("read kafka sasl password_file: %w", err)
	}
	password := strings.TrimSpace(string(passwordBytes))
	authFn := func(context.Context) (plain.Auth, error) {
		return plain.Auth{User: cfg.Username, Pass: password}, nil
	}

	var mechanism sasl.Mechanism
	switch cfg.Mechanism {
	case "PLAIN":
		mechanism = plain.Plain(authFn)
	case "SCRAM-SHA-256":
		mechanism = scram.Sha256(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: cfg.Username, Pass: password}, nil
		})
	case "SCRAM-SHA-512":
		mechanism = scram.Sha512(func(context.Context) (scram.Auth, error) {
			return scram.Auth{User: cfg.Username, Pass: password}, nil
		})
	default:
		return nil, fmt.Errorf("unsupported kafka sasl mechanism %q", cfg.Mechanism)
	}
	return []kgo.Opt{kgo.SASL(mechanism)}, nil
}
