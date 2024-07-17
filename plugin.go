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

type Configurer interface {
    // UnmarshalKey takes a single key and unmarshal it into a Struct.
    UnmarshalKey(name string, out any) error
    // Has checks if config section exists.
    Has(name string) bool
}

type Logger interface {
    NamedLogger(name string) *zap.Logger
}

type Plugin struct {
    cfg      *Config
    client   pulsar.Client
    producer pulsar.Producer
    consumer pulsar.Consumer
    wg       sync.WaitGroup
    log      *zap.Logger
}

// LoadConfig loads the plugin configuration.
func (p *Plugin) LoadConfig(configData []byte) error {
    p.log.Debug("Loading plugin configuration")
    cfg := &Config{}
    err := yaml.Unmarshal(configData, cfg)
    if err != nil {
        return fmt.Errorf("could not unmarshal config: %w", err)
    }

    cfg.InitDefaults()
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

    p.log.Debug("Plugin configuration loaded successfully")
    return nil
}

func (p *Plugin) Init(cfg Configurer, log Logger) error {
    p.log = log.NamedLogger(PluginName)
    p.log.Debug("Initializing Pulsar plugin")

    config := &Config{}
    if err := cfg.UnmarshalKey("pulsar", config); err != nil {
        return fmt.Errorf("could not unmarshal plugin config: %w", err)
    }

    config.InitDefaults()
    p.cfg = config

    client, err := pulsar.NewClient(pulsar.ClientOptions{
        URL: config.URL,
    })
    if err != nil {
        return fmt.Errorf("could not instantiate Pulsar client: %w", err)
    }
    p.client = client

    return nil
}

func (p *Plugin) Name() string {
    p.log.Debug("Name")
    return PluginName
}

func (p *Plugin) Serve() chan error {
    p.log.Debug("Starting the serve")
    p.log.Debug("Pulsar plugin is starting")
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

func (p *Plugin) Stop() error {
    p.log.Debug("Stopping Pulsar plugin")
    p.producer.Close()
    p.consumer.Close()
    p.client.Close()
    p.wg.Wait()
    p.log.Debug("Pulsar plugin has stopped")
    return nil
}

func (p *Plugin) listenForMessages() error {
    ctx := context.Background()
    p.log.Debug("Listening for messages")
    for {
        msg, err := p.consumer.Receive(ctx)
        if err != nil {
            return fmt.Errorf("failed to receive message: %w", err)
        }

        fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
            msg.ID(), string(msg.Payload()))

        p.consumer.Ack(msg)
        p.log.Debug("Message acknowledged")
    }
}
