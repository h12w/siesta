package siesta

import (
	"io"
	"math/rand"
	"time"
)

type SyncProducer struct {
	c Connector
	*SyncProducerConfig
}

type SyncProducerConfig struct {
	Addrs []string
	*ProducerConfig
	Topic          string
	PartitionCount int
}

func NewSyncProducerConfig() *SyncProducerConfig {
	return &SyncProducerConfig{
		ProducerConfig: &ProducerConfig{
			BatchSize:       1,
			ClientID:        "siesta",
			MaxRequests:     10,
			SendRoutines:    10,
			ReceiveRoutines: 10,
			ReadTimeout:     5 * time.Second,
			WriteTimeout:    5 * time.Second,
			RequiredAcks:    1,
			AckTimeoutMs:    2000,
			Linger:          1 * time.Second,
		},
	}
}

func NewSyncProducer(config *SyncProducerConfig) (*SyncProducer, error) {
	cfg := NewConnectorConfig()
	cfg.BrokerList = config.Addrs
	c, err := NewDefaultConnector(cfg)
	if err != nil {
		return nil, err
	}
	return &SyncProducer{
		SyncProducerConfig: config,
		c:                  c,
	}, nil
}

func (p *SyncProducer) Send(v []byte) error {
	partition := int32(rand.Intn(p.PartitionCount))
	leader, err := p.c.GetLeader(p.Topic, partition)
	if err != nil {
		return err
	}
	correlationID, conn, err := leader.GetConnection()
	if err != nil {
		return err
	}
	defer leader.ReturnConnection(conn)
	{
		request := new(ProduceRequest)
		request.RequiredAcks = 1
		request.AckTimeoutMs = p.AckTimeoutMs
		if err != nil {
			return err
		}
		request.AddMessage(p.Topic, partition, &Message{Key: nil, Value: v})

		writer := NewRequestHeader(correlationID, p.ClientID, request)
		bytes := make([]byte, writer.Size())
		encoder := NewBinaryEncoder(bytes)
		writer.Write(encoder)
		conn.SetWriteDeadline(time.Now().Add(p.WriteTimeout))
		if _, err := conn.Write(bytes); err != nil {
			return err
		}
	}
	{
		conn.SetReadDeadline(time.Now().Add(p.ReadTimeout))
		header := make([]byte, 8)
		_, err := io.ReadFull(conn, header)
		if err != nil {
			return err
		}

		length, err := NewBinaryDecoder(header).GetInt32()
		if err != nil {
			return err
		}
		response := make([]byte, length-4)
		_, err = io.ReadFull(conn, response)
		if err != nil {
			return err
		}

		produceResponse := new(ProduceResponse)
		if err := produceResponse.Read(NewBinaryDecoder(response)); err != nil {
			return err.Error()
		}
		if err := produceResponse.Status[p.Topic][partition].Error; err != nil && err != ErrNoError {
			return err
		}
	}
	return nil
}
