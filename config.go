package pulsarplugin

type Config struct {
    URL              string `yaml:"url"`
    Topic            string `yaml:"topic"`
    SubscriptionName string `yaml:"subscription"`
}

// InitDefaults initializing fill config with default values
func (s *Config) InitDefaults() {
    if s.URL == "" {
        s.URL = "pulsar://localhost:6650"
    }

    if s.Topic == "" {
        s.Topic = "default-topic"
    }

    if s.SubscriptionName == "" {
        s.SubscriptionName = "default-subscription"
    }
}