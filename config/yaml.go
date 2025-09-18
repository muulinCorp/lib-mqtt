package config

import (
	"fmt"
	"net/url"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type yamlConfig struct {
	ServerURL         string        `yaml:"server_url"`
	Auth              *authConf     `yaml:"auth"`
	Qos               byte          `yaml:"qos"`
	KeepAlive         uint16        `yaml:"keep_alive"`
	ConnectRetryDelay time.Duration `yaml:"connect_retry_delay"`
	QueuePath         string        `yaml:"queue_path"`
	Debug             bool          `yaml:"debug"`
}

func GetConfigFromYaml(file string, service string) (*Config, error) {
	var yamlCfg yamlConfig
	var err error
	data, err := os.ReadFile(file)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(data, &yamlCfg)
	if err != nil {
		return nil, err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	var cfg Config
	myurl, err := url.Parse(yamlCfg.ServerURL)
	if err != nil {
		return nil, err
	}
	cfg.ServerURL = myurl
	cfg.ClientID = fmt.Sprintf("%s-%s", service, hostname)
	cfg.Qos = yamlCfg.Qos
	cfg.KeepAlive = yamlCfg.KeepAlive
	cfg.ConnectRetryDelay = yamlCfg.ConnectRetryDelay
	cfg.QueuePath = yamlCfg.QueuePath
	cfg.Debug = yamlCfg.Debug
	cfg.Auth = yamlCfg.Auth

	return &cfg, nil
}
