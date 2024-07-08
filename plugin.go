package pulsarplugin

import (
    "context"
    "fmt"
    "sync"

    "github.com/apache/pulsar-client-go/pulsar"
    "go.uber.org/zap"
    "gopkg.in/yaml.v2"
)

const PluginName = "pulsarplugin"

type PulsarPlugin struct {
    cfg      *Config
    client   pulsar.Client
    producer pulsar.Producer
    consumer pulsar.Consumer
    wg       sync.WaitGroup
    log      *zap.Logger
}

// LoadConfig loads the plugin configuration.
func (p *PulsarPlugin) LoadConfig(configData []byte) error {
    p.log.Info("Loading plugin configuration")
    cfg := &Config{}
    err := yaml.Unmarshal(configData, cfg)
    if err != nil {
        return fmt.Errorf("could not unmarshal config: %w", err)
    }

    p.cfg = cfg

    client, err := pulsar.NewClient(pulsar.ClientOptions{
        URL: cfg.URL,
    })
    if err != nil {
        return fmt.Errorf("could not instantiate Pulsar client: %w", err)
    }
    p.client = client

    producer, err := client.CreateProducer(pulsar.ProducerOptions{
        Topic: cfg.Topic,
    })
    if err != nil {
        return fmt.Errorf("could not create Pulsar producer: %w", err)
    }
    p.producer = producer

    consumer, err := client.Subscribe(pulsar.ConsumerOptions{
        Topic:            cfg.Topic,
        SubscriptionName: cfg.SubscriptionName,
    })
    if err != nil {
        return fmt.Errorf("could not create Pulsar consumer: %w", err)
    }
    p.consumer = consumer

    p.log.Info("Plugin configuration loaded successfully")
    return nil
}

func (p *PulsarPlugin) Init(log *zap.Logger) error {
    p.log = log
    p.log.Info("Initializing Pulsar plugin")

    cfg := &Config{}

    p.cfg = cfg

    client, err := pulsar.NewClient(pulsar.ClientOptions{
        URL: cfg.URL,
    })
    if err != nil {
        return fmt.Errorf("could not instantiate Pulsar client: %w", err)
    }
    p.client = client

    producer, err := client.CreateProducer(pulsar.ProducerOptions{
        Topic: cfg.Topic,
    })
    if err != nil {
        return fmt.Errorf("could not create Pulsar producer: %w", err)
    }
    p.producer = producer

    consumer, err := client.Subscribe(pulsar.ConsumerOptions{
        Topic:            cfg.Topic,
        SubscriptionName: cfg.SubscriptionName,
    })
    if err != nil {
        return fmt.Errorf("could not create Pulsar consumer: %w", err)
    }
    p.consumer = consumer

    return nil
}

func (p *Plugin) Name() string {
	return PluginName
}

func (p *PulsarPlugin) Serve() chan error {
    p.log.Info("Pulsar plugin is starting")
    errCh := make(chan error, 1)

    p.wg.Add(1)
    go func() {
        defer p.wg.Done()
        err := p.listenForMessages()
        if err != nil {
            errCh <- err
        }
    }()

    return errCh
}

func (p *PulsarPlugin) Stop() error {
    p.log.Info("Stopping Pulsar plugin")
    p.producer.Close()
    p.consumer.Close()
    p.client.Close()
    p.wg.Wait()
    p.log.Info("Pulsar plugin has stopped")
    return nil
}

func (p *PulsarPlugin) listenForMessages() error {
    ctx := context.Background()
    p.log.Info("Listening for messages")
    for {
        msg, err := p.consumer.Receive(ctx)
        if err != nil {
            return fmt.Errorf("failed to receive message: %w", err)
        }

        fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
            msg.ID(), string(msg.Payload()))

        p.consumer.Ack(msg)
        p.log.Info("Message acknowledged")
    }
}
