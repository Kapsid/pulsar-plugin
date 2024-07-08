package pulsarplugin

import "time"

type Config struct {
    URL              string `yaml:"url"`
    Topic            string `yaml:"topic"`
    SubscriptionName string `yaml:"subscription"`
}

// InitDefaults initializing fill config with default values
func (s *Config) InitDefaults() {
	if s.Addrs == nil {
		s.Addrs = []string{"127.0.0.1:6379"} // default addr is pointing to local storage
	}
}