package config

import (
	"fmt"
	"log"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Retrieve config from environmental variables

// Configuration will be pulled from the environment using the following keys
const (
	envServerURL = "MQTT_BrokerURL" // server URL

	envAuth         = "MQTT_IsAuth"
	envAuthUser     = "MQTT_User"
	envAuthPassword = "MQTT_Password"

	envTopics = "MQTT_Topic" // topic to publish on
	envClient = "MQTT_ClientID"
	envGroup  = "MQTT_Group" // topic to publish on
	envQos    = "MQTT_Qos"   // qos to utilise when publishing

	envKeepAlive         = "MQTT_KeepAlive"         // seconds between keepalive packets
	envConnectRetryDelay = "MQTT_ConnectRetryDelay" // milliseconds to delay between connection attempts
	envEnableGzip        = "MQTT_Gzip"
	envQueuePath         = "MQTT_QueuePath"

	envStoreType   = "MQTT_Store_Type"
	envStorePath   = "MQTT_Store_Path"
	envStoreEnable = "MQTT_Store_Enable"

	envThrottledQueueDuration = "MQTT_ThrottledQueue_Duration"

	envDebug = "MQTT_Debug" // if "true" then the libraries will be instructed to print debug info
)

type authConf struct {
	UserName string
	Password []byte
}

type storeConf struct {
	Type string
	Path string
}

// config holds the configuration
type Config struct {
	ServerURL *url.URL // MQTT server URL
	Auth      *authConf
	Store     *storeConf
	Group     string
	ClientID  string   // Client ID to use when connecting to server
	Topics    []string // Topic on which to publish messaged
	Qos       byte     // QOS to use when publishing

	KeepAlive         uint16        // seconds between keepalive packets
	ConnectRetryDelay time.Duration // Period between connection attempts

	QueuePath  string
	EnableGzip bool
	ThrottledQueueDuration time.Duration

	Debug bool // autopaho and paho debug output requested

	Logger *log.Logger
}

func (c *Config) getGroupTopic(t string) string {
	return fmt.Sprintf("$share/%s/%s", c.Group, t)
}

func (c *Config) AddTopics(topics ...string) {
	if c.Group != "" {
		for _, t := range topics {
			c.Topics = append(c.Topics, c.getGroupTopic(t))
		}
	} else {
		c.Topics = append(c.Topics, topics...)
	}
}

func (c *Config) SetAuth(user string, pass []byte) {
	c.Auth = &authConf{UserName: user, Password: pass}
}

func (c *Config) SetStore(typ string, path string) {
	c.Store = &storeConf{Type: typ, Path: path}
}

func (c *Config) IsRawdata() bool {
	for _, t := range c.Topics {
		if strings.Contains(t, "v1/rawdata") {
			return true
		}
	}
	return false
}

// getConfig - Retrieves the configuration from the environment
func GetConfigFromEnv() (*Config, error) {
	var cfg Config
	var err error

	var srvURL string
	if srvURL, err = stringFromEnv(envServerURL); err != nil {
		return nil, err
	}

	cfg.ServerURL, err = url.Parse(srvURL)
	if err != nil {
		return nil, fmt.Errorf("environmental variable %s must be a valid URL (%w)", envServerURL, err)
	}
	name, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	if clientID, err := stringFromEnv(envClient); err != nil {
		return nil, err
	} else {
		cfg.ClientID = fmt.Sprintf("%s-%s", clientID, name)
	}

	topics, err := stringFromEnv(envTopics)
	if err != nil {
		return nil, err
	}
	cfg.Topics = strings.Split(topics, "&")

	group, _ := stringFromEnv(envGroup)

	if group != "" {
		cfg.Group = group
		shareTopics := make([]string, len(cfg.Topics))
		for i, t := range cfg.Topics {
			shareTopics[i] = cfg.getGroupTopic(t)
		}
		cfg.Topics = shareTopics
	}

	cfg.QueuePath, _ = stringFromEnv(envQueuePath)

	iQos, err := intFromEnv(envQos)
	if err != nil {
		return nil, err
	}
	cfg.Qos = byte(iQos)

	iKa, err := intFromEnv(envKeepAlive)
	if err != nil {
		return nil, err
	}
	cfg.KeepAlive = uint16(iKa)

	if cfg.ConnectRetryDelay, err = milliSecondsFromEnv(envConnectRetryDelay); err != nil {
		return nil, err
	}

	if cfg.Debug, err = booleanFromEnv(envDebug); err != nil {
		return nil, err
	}

	if cfg.Debug {
		cfg.Logger = log.New(os.Stdout, "", 0)
	}

	isAuth, err := booleanFromEnv(envAuth)
	if err != nil {
		return nil, err
	}
	if isAuth {
		cfg.Auth = &authConf{}
		if cfg.Auth.UserName, err = stringFromEnv(envAuthUser); err != nil {
			return nil, err
		}
		password, err := stringFromEnv(envAuthPassword)
		if err != nil {
			return nil, err
		}
		cfg.Auth.Password = []byte(password)
	}
	cfg.EnableGzip, err = booleanFromEnv(envEnableGzip)
	if err != nil {
		return nil, err
	}

	isStoreEnable, err := booleanFromEnv(envStoreEnable)
	if err != nil {
		return nil, err
	}
	if isStoreEnable {
		storeType, err := stringFromEnv(envStoreType)
		if err != nil {
			return nil, err
		}
		storePath, err := stringFromEnv(envStorePath)
		if err != nil {
			return nil, err
		}
		if storeType != "" && storeType != "memory" && storeType != "file" {
			return nil, fmt.Errorf("environmental variable %s must be 'memory' or 'file'", envStoreType)
		}
		if storePath != "" && storeType != "" {
			cfg.Store = &storeConf{Type: storeType, Path: storePath}
		}
	}

		throttleDurationString, _ := stringFromEnv(envThrottledQueueDuration)
	if throttleDurationString == "" {
		cfg.ThrottledQueueDuration = 30*time.Second
	} else {
		throttleDuration, err := time.ParseDuration(throttleDurationString)
		if err != nil {
			return nil, err
		}
		if throttleDuration < (time.Second * 5) {
			return nil, fmt.Errorf("ThrottledQueueDuration must be at least 5 seconds")
		}
		cfg.ThrottledQueueDuration = throttleDuration
	}

	return &cfg, nil
}

func GetConfigFromEnvWithoutTopic() (*Config, error) {
	var cfg Config
	var err error

	var srvURL string
	if srvURL, err = stringFromEnv(envServerURL); err != nil {
		return nil, err
	}

	cfg.ServerURL, err = url.Parse(srvURL)
	if err != nil {
		return nil, fmt.Errorf("environmental variable %s must be a valid URL (%w)", envServerURL, err)
	}
	name, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	if clientID, err := stringFromEnv(envClient); err != nil {
		return nil, err
	} else {
		cfg.ClientID = fmt.Sprintf("%s-%s", clientID, name)
	}

	group, _ := stringFromEnv(envGroup)
	if group != "" {
		cfg.Group = group
	}

	cfg.QueuePath, _ = stringFromEnv(envQueuePath)

	iQos, err := intFromEnv(envQos)
	if err != nil {
		return nil, err
	}
	cfg.Qos = byte(iQos)

	iKa, err := intFromEnv(envKeepAlive)
	if err != nil {
		return nil, err
	}
	cfg.KeepAlive = uint16(iKa)

	if cfg.ConnectRetryDelay, err = milliSecondsFromEnv(envConnectRetryDelay); err != nil {
		return nil, err
	}

	if cfg.Debug, err = booleanFromEnv(envDebug); err != nil {
		return nil, err
	}

	if cfg.Debug {
		cfg.Logger = log.New(os.Stdout, "", 0)
	}

	isAuth, err := booleanFromEnv(envAuth)
	if err != nil {
		return nil, err
	}
	if isAuth {
		cfg.Auth = &authConf{}
		if cfg.Auth.UserName, err = stringFromEnv(envAuthUser); err != nil {
			return nil, err
		}
		password, err := stringFromEnv(envAuthPassword)
		if err != nil {
			return nil, err
		}
		cfg.Auth.Password = []byte(password)
	}
	cfg.EnableGzip, err = booleanFromEnv(envEnableGzip)
	if err != nil {
		return nil, err
	}

	throttleDurationString, _ := stringFromEnv(envThrottledQueueDuration)
	if throttleDurationString == "" {
		cfg.ThrottledQueueDuration = 30*time.Second
	} else {
		throttleDuration, err := time.ParseDuration(throttleDurationString)
		if err != nil {
			return nil, err
		}
		if throttleDuration < (time.Second * 5) {
			return nil, fmt.Errorf("ThrottledQueueDuration must be at least 5 seconds")
		}
		cfg.ThrottledQueueDuration = throttleDuration
	}

	return &cfg, nil
}

// stringFromEnv - Retrieves a string from the environment and ensures it is not blank (ort non-existent)
func stringFromEnv(key string) (string, error) {
	s := os.Getenv(key)
	if len(s) == 0 {
		return "", fmt.Errorf("environmental variable %s must not be blank", key)
	}
	return s, nil
}

// intFromEnv - Retrieves an integer from the environment (must be present and valid)
func intFromEnv(key string) (int, error) {
	s := os.Getenv(key)
	if len(s) == 0 {
		return 0, fmt.Errorf("environmental variable %s must not be blank", key)
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("environmental variable %s must be an integer", key)
	}
	return i, nil
}

// milliSecondsFromEnv - Retrieves milliseconds (as time.Duration) from the environment (must be present and valid)
func milliSecondsFromEnv(key string) (time.Duration, error) {
	s := os.Getenv(key)
	if len(s) == 0 {
		return 0, fmt.Errorf("environmental variable %s must not be blank", key)
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("environmental variable %s must be an integer", key)
	}
	return time.Duration(i) * time.Millisecond, nil
}

// booleanFromEnv - Retrieves boolean from the environment (must be present and valid)
func booleanFromEnv(key string) (bool, error) {
	s := os.Getenv(key)
	if len(s) == 0 {
		return false, fmt.Errorf("environmental variable %s must not be blank", key)
	}
	switch strings.ToUpper(s) {
	case "TRUE", "T", "1":
		return true, nil
	case "FALSE", "F", "0":
		return false, nil
	default:
		return false, fmt.Errorf("environmental variable %s be a valid boolean option (is %s)", key, s)
	}
}

func envHasPrefix(prefix string, env string) string {
	if prefix == "" {
		return env
	}
	return prefix + "_" + env
}

func GetConfigFromEnvPrefixWithoutTopic(prefix string) (*Config, error) {
	if prefix == "" {
		return GetConfigFromEnvWithoutTopic()
	}
	var cfg Config
	var err error

	var srvURL string
	if srvURL, err = stringFromEnv(envHasPrefix(prefix, envServerURL)); err != nil {
		return nil, err
	}

	cfg.ServerURL, err = url.Parse(srvURL)
	if err != nil {
		return nil, fmt.Errorf("environmental variable %s must be a valid URL (%w)", envServerURL, err)
	}
	name, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	if clientID, err := stringFromEnv(envHasPrefix(prefix, envClient)); err != nil {
		return nil, err
	} else {
		cfg.ClientID = fmt.Sprintf("%s-%s", clientID, name)
	}

	group, _ := stringFromEnv(envHasPrefix(prefix, envGroup))
	if group != "" {
		cfg.Group = group
	}

	cfg.QueuePath, _ = stringFromEnv(envHasPrefix(prefix, envQueuePath))

	iQos, err := intFromEnv(envHasPrefix(prefix, envQos))
	if err != nil {
		return nil, err
	}
	cfg.Qos = byte(iQos)

	iKa, err := intFromEnv(envHasPrefix(prefix, envKeepAlive))
	if err != nil {
		return nil, err
	}
	cfg.KeepAlive = uint16(iKa)

	if cfg.ConnectRetryDelay, err = milliSecondsFromEnv(envHasPrefix(prefix, envConnectRetryDelay)); err != nil {
		return nil, err
	}

	if cfg.Debug, err = booleanFromEnv(envHasPrefix(prefix, envDebug)); err != nil {
		return nil, err
	}

	if cfg.Debug {
		cfg.Logger = log.New(os.Stdout, "", 0)
	}

	isAuth, err := booleanFromEnv(envHasPrefix(prefix, envAuth))
	if err != nil {
		return nil, err
	}
	if isAuth {
		cfg.Auth = &authConf{}
		if cfg.Auth.UserName, err = stringFromEnv(envHasPrefix(prefix, envAuthUser)); err != nil {
			return nil, err
		}
		password, err := stringFromEnv(envHasPrefix(prefix, envAuthPassword))
		if err != nil {
			return nil, err
		}
		cfg.Auth.Password = []byte(password)
	}

	return &cfg, nil
}
