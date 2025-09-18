package mqtt

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/tls"

	// "encoding/gob"
	"errors"
	"io"
	"log"
	"net/url"
	"os"

	// "sync"

	"github.com/muulinCorp/lib-mqtt/config"
	"github.com/muulinCorp/lib-mqtt/trans"

	"github.com/eclipse/paho.golang/autopaho"
	"github.com/eclipse/paho.golang/autopaho/queue"

	// "github.com/eclipse/paho.golang/autopaho/queue/file"
	// "github.com/eclipse/paho.golang/packets"
	"github.com/eclipse/paho.golang/paho"
	"github.com/eclipse/paho.golang/paho/session"
	"github.com/eclipse/paho.golang/paho/session/state"
	storefile "github.com/eclipse/paho.golang/paho/store/file"
	"github.com/eclipse/paho.golang/paho/store/memory"
)

type MqttServer interface {
	PublishViaQueue(ctx context.Context, topic string, qos byte, payload []byte) error
	Publish(ctx context.Context, topic string, qos byte, payload []byte) error
	MqttSubOnlyServer
}

type MqttSubOnlyServer interface {
	Run(ctx context.Context)
	Close()
	SetLog(l *log.Logger)
	Statue() error
}

func createDirIfNotExists(path string, perm os.FileMode) error {
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return os.MkdirAll(path, perm)
	}
	return nil
}

func NewMqttPublishOnlyServ(conf *config.Config) (MqttServer, error) {
	err := createDirIfNotExists(conf.QueuePath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	conf.Topics = []string{}
	return &mqttServ{
		config: conf,
	}, nil
}

func NewMqttSubOnlyServ(conf *config.Config, tsmap map[string]trans.Trans) (MqttSubOnlyServer, error) {
	err := createDirIfNotExists(conf.QueuePath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &mqttServ{
		config: conf,
		h:      NewHandler(conf, tsmap),
	}, nil
}

func NewMqttServ(conf *config.Config, tsmap map[string]trans.Trans) (MqttServer, error) {
	err := createDirIfNotExists(conf.QueuePath, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return &mqttServ{
		config: conf,
		h:      NewHandler(conf, tsmap),
	}, nil
}

type mqttServ struct {
	config      *config.Config
	cm          *autopaho.ConnectionManager
	h           Handler
	isConnected bool
}

func (serv *mqttServ) Statue() error {
	if serv.isConnected {
		return nil
	}
	return errors.New("not connected")
}

func (serv *mqttServ) Close() {
	if serv.h != nil {
		serv.h.Close()
	}
}

func (serv *mqttServ) Run(ctx context.Context) {
	var err error
	cfg := serv.config
	var q queue.Queue
	var session session.SessionManager
	if cfg.QueuePath != "" {
		q, err = NewThrottledQueue(cfg.QueuePath, cfg.ThrottledQueueDuration)
		if err != nil {
			panic(err)
		}

		if cfg.Store != nil {
			switch cfg.Store.Type {
			case "memory":
				clientStore := memory.New()
				serverStore := memory.New()
				session = state.New(clientStore, serverStore)
			case "file":
				// check path exists
				if _, err := os.Stat(cfg.Store.Path); os.IsNotExist(err) {
					err = os.MkdirAll(cfg.Store.Path, os.ModePerm)
					if err != nil {
						panic(err)
					}
				}
				clientStore, err := storefile.New(cfg.Store.Path, "client", ".session")
				if err != nil {
					panic(err)
				}
				serverStore, err := storefile.New(cfg.Store.Path, "server", ".session")
				if err != nil {
					panic(err)
				}
				session = state.New(clientStore, serverStore)
			default:
				panic("unknown store type")
			}
			if cfg.Logger != nil {
				session.SetDebugLogger(cfg.Logger)
				session.SetErrorLogger(cfg.Logger)
			}
		}
	}

	cliCfg := autopaho.ClientConfig{
		ServerUrls:                    []*url.URL{cfg.ServerURL},
		CleanStartOnInitialConnection: false,
		KeepAlive:                     20,
		// SessionExpiryInterval - Seconds that a session will survive after disconnection.
		// It is important to set this because otherwise, any queued messages will be lost if the connection drops and
		// the server will not queue messages while it is down. The specific setting will depend upon your needs
		// (60 = 1 minute, 3600 = 1 hour, 86400 = one day, 0xFFFFFFFE = 136 years, 0xFFFFFFFF = don't expire)
		SessionExpiryInterval: 60,
		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
			serv.println("mqtt connection up")
			serv.isConnected = true
			// Subscribing in the OnConnectionUp callback is recommended (ensures the subscription is reestablished if
			// the connection drops)
			if len(cfg.Topics) == 0 {
				return
			}

			subOpts := make([]paho.SubscribeOptions, len(cfg.Topics))
			for i, t := range cfg.Topics {
				subOpts[i] = paho.SubscribeOptions{Topic: t, QoS: cfg.Qos}
			}
			if _, err := cm.Subscribe(ctx, &paho.Subscribe{
				Subscriptions: subOpts,
			}); err != nil {
				serv.printf("failed to subscribe (%s). This is likely to mean no messages will be received.", err)
			}
			serv.println("mqtt subscription made")
		},
		OnConnectError: func(err error) { serv.printf("error whilst attempting connection: %s\n", err) },
		ClientConfig: paho.ClientConfig{
			// If you are using QOS 1/2, then it's important to specify a client id (which must be unique)
			ClientID: cfg.ClientID,
			Session:  session,
			// OnPublishReceived is a slice of functions that will be called when a message is received.
			// You can write the function(s) yourself or use the supplied Router
			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
				func(pr paho.PublishReceived) (bool, error) {
					serv.printf("received message on topic %s; retain: %t\n", pr.Packet.Topic, pr.Packet.Retain)
					if serv.h == nil {
						return true, nil
					}
					serv.h.handle(&pr)
					return true, nil
				},
			},
			OnClientError: func(err error) { serv.printf("client error: %s\n", err) },
			OnServerDisconnect: func(d *paho.Disconnect) {
				serv.isConnected = false
				if d.Properties != nil {
					serv.printf("server requested disconnect: %s\n", d.Properties.ReasonString)
				} else {
					serv.printf("server requested disconnect; reason code: %d\n", d.ReasonCode)
				}
			},
		},
		Queue: q,
	}
	if cfg.Debug && serv.config.Logger == nil {
		panic("logger not set")
	}
	if cfg.Debug {
		cliCfg.Debug = serv.config.Logger
		cliCfg.PahoDebug = serv.config.Logger
	}

	if cfg.Auth != nil {
		cliCfg.ConnectPassword = cfg.Auth.Password
		cliCfg.ConnectUsername = cfg.Auth.UserName
	}

	if cfg.ServerURL.Scheme == "mqtts" {
		cliCfg.TlsCfg = &tls.Config{
			ClientAuth:         tls.NoClientCert,
			ClientCAs:          nil,
			InsecureSkipVerify: true,
		}
	}

	// Connect to the broker
	serv.cm, err = autopaho.NewConnection(ctx, cliCfg)
	if err != nil {
		panic(err)
	}

	// Wait for the connection to come up
	if err = serv.cm.AwaitConnection(ctx); err != nil {
		panic(err)
	}

	// Messages will be handled through the callback so we really just need to wait until a shutdown
	// is requested
	<-ctx.Done()
	serv.println("signal caught - exiting subscribe")
	serv.println("shutdown subscribe complete")
}

///	NOTE another implementation of solution for managing mqtt's queue backpressure
// func (serv *mqttServ) Run(ctx context.Context) {
// 	var err error
// 	cfg := serv.config
// 	var q queue.Queue
// 	var session session.SessionManager
// 	var publishStarted bool
// 	var publishMutex sync.Mutex

// 	if cfg.QueuePath != "" {
// 		q, err = file.New(cfg.QueuePath, "queue", ".msg")
// 		if err != nil {
// 			panic(err)
// 		}

// 		if cfg.Store != nil {
// 			switch cfg.Store.Type {
// 			case "memory":
// 				clientStore := memory.New()
// 				serverStore := memory.New()
// 				session = state.New(clientStore, serverStore)
// 			case "file":
// 				// check path exists
// 				if _, err := os.Stat(cfg.Store.Path); os.IsNotExist(err) {
// 					err = os.MkdirAll(cfg.Store.Path, os.ModePerm)
// 					if err != nil {
// 						panic(err)
// 					}
// 				}
// 				clientStore, err := storefile.New(cfg.Store.Path, "client", ".session")
// 				if err != nil {
// 					panic(err)
// 				}
// 				serverStore, err := storefile.New(cfg.Store.Path, "server", ".session")
// 				if err != nil {
// 					panic(err)
// 				}
// 				session = state.New(clientStore, serverStore)
// 			default:
// 				panic("unknown store type")
// 			}
// 			if cfg.Logger != nil {
// 				session.SetDebugLogger(cfg.Logger)
// 				session.SetErrorLogger(cfg.Logger)
// 			}
// 		}
// 	}

// 	cliCfg := autopaho.ClientConfig{
// 		ServerUrls:                    []*url.URL{cfg.ServerURL},
// 		CleanStartOnInitialConnection: false,
// 		KeepAlive:                     20,
// 		// SessionExpiryInterval - Seconds that a session will survive after disconnection.
// 		// It is important to set this because otherwise, any queued messages will be lost if the connection drops and
// 		// the server will not queue messages while it is down. The specific setting will depend upon your needs
// 		// (60 = 1 minute, 3600 = 1 hour, 86400 = one day, 0xFFFFFFFE = 136 years, 0xFFFFFFFF = don't expire)
// 		SessionExpiryInterval: 60,
// 		OnConnectionUp: func(cm *autopaho.ConnectionManager, connAck *paho.Connack) {
// 			publishMutex.Lock()
// 			if publishStarted {
// 				publishMutex.Unlock()
// 				return
// 			}
// 			publishStarted = true
// 			publishMutex.Unlock()

// 			serv.println("mqtt connection up")
// 			serv.isConnected = true
// 			// Subscribing in the OnConnectionUp callback is recommended (ensures the subscription is reestablished if
// 			// the connection drops)
// 			if len(cfg.Topics) == 0 {
// 				return
// 			}

// 			go func() {
// 				ticker := time.NewTicker(time.Minute)
// 				defer ticker.Stop()

// 				for {
// 					entry, err := q.Peek()
// 					if err != nil {
// 						if errors.Is(err, queue.ErrEmpty) {
// 							break
// 						}
// 						serv.printf("error peeking queue: %s\n", err)
// 						continue
// 					}

// 					ioReader, err := entry.Reader()
// 					if err != nil {
// 						serv.printf("error get entry reader: %s\n", err)
// 						continue
// 					}

// 					// pkt, err := packets.ReadPacket(ioReader)
// 					// if err != nil {
// 					// 	serv.printf("error reading queue item: %s\n", err)
// 					// 	continue
// 					// }

// 					// var b bytes.Buffer
// 					// err = pkt.Content.Unpack(&b)
// 					// if err != nil {
// 					// 	serv.printf("error reading queue item: %s\n", err)
// 					// 	continue
// 					// }
// 					var tempP packets.Publish
// 					dec := gob.NewDecoder(ioReader)
// 					err = dec.Decode(&tempP)
// 					if err != nil {
// 						serv.printf("error reading queue item: %s\n", err)
// 						continue
// 					}

// 					err = serv.Publish(ctx, tempP.Topic, tempP.QoS, tempP.Payload)
// 					if err != nil {
// 						serv.printf("error publishing queue item: %s\n", err)
// 						continue
// 					}

// 					err = entry.Remove()
// 					if err != nil {
// 						if errors.Is(err, queue.ErrEmpty) {
// 							break
// 						}
// 						serv.printf("error removing queue item: %s\n", err)
// 					}

// 					<- ticker.C
// 				}
// 				serv.println("mqtt done of re-publish item in queue")

// 				publishMutex.Lock()
// 				publishStarted = false
// 				publishMutex.Unlock()

// 			subOpts := make([]paho.SubscribeOptions, len(cfg.Topics))
// 			for i, t := range cfg.Topics {
// 				subOpts[i] = paho.SubscribeOptions{Topic: t, QoS: cfg.Qos}
// 			}
// 			if _, err := cm.Subscribe(ctx, &paho.Subscribe{
// 				Subscriptions: subOpts,
// 			}); err != nil {
// 				serv.printf("failed to subscribe (%s). This is likely to mean no messages will be received.", err)
// 			}
// 			serv.println("mqtt subscription made")
// 			} ()
// 		},
// 		OnConnectError: func(err error) { serv.printf("error whilst attempting connection: %s\n", err) },
// 		ClientConfig: paho.ClientConfig{
// 			// If you are using QOS 1/2, then it's important to specify a client id (which must be unique)
// 			ClientID: cfg.ClientID,
// 			Session:  session,
// 			// OnPublishReceived is a slice of functions that will be called when a message is received.
// 			// You can write the function(s) yourself or use the supplied Router
// 			OnPublishReceived: []func(paho.PublishReceived) (bool, error){
// 				func(pr paho.PublishReceived) (bool, error) {
// 					serv.printf("received message on topic %s; retain: %t\n", pr.Packet.Topic, pr.Packet.Retain)
// 					if serv.h == nil {
// 						return true, nil
// 					}
// 					serv.h.handle(&pr)
// 					return true, nil
// 				},
// 			},
// 			OnClientError: func(err error) { serv.printf("client error: %s\n", err) },
// 			OnServerDisconnect: func(d *paho.Disconnect) {
// 				serv.isConnected = false
// 				if d.Properties != nil {
// 					serv.printf("server requested disconnect: %s\n", d.Properties.ReasonString)
// 				} else {
// 					serv.printf("server requested disconnect; reason code: %d\n", d.ReasonCode)
// 				}
// 			},
// 		},
// 	}
// 	if cfg.Debug && serv.config.Logger == nil {
// 		panic("logger not set")
// 	}
// 	if cfg.Debug {
// 		cliCfg.Debug = serv.config.Logger
// 		cliCfg.PahoDebug = serv.config.Logger
// 	}

// 	if cfg.Auth != nil {
// 		cliCfg.ConnectPassword = cfg.Auth.Password
// 		cliCfg.ConnectUsername = cfg.Auth.UserName
// 	}

// 	if cfg.ServerURL.Scheme == "mqtts" {
// 		cliCfg.TlsCfg = &tls.Config{
// 			ClientAuth:         tls.NoClientCert,
// 			ClientCAs:          nil,
// 			InsecureSkipVerify: true,
// 		}
// 	}

// 	// Connect to the broker
// 	serv.cm, err = autopaho.NewConnection(ctx, cliCfg)
// 	if err != nil {
// 		panic(err)
// 	}

// 	// Wait for the connection to come up
// 	if err = serv.cm.AwaitConnection(ctx); err != nil {
// 		panic(err)
// 	}

// 	// Messages will be handled through the callback so we really just need to wait until a shutdown
// 	// is requested
// 	<-ctx.Done()
// 	serv.println("signal caught - exiting subscribe")
// 	serv.println("shutdown subscribe complete")
// }

func (serv *mqttServ) Publish(ctx context.Context, topic string, qos byte, payload []byte) error {
	// Publish will block so we run it in a goRoutine
	var err error
	if serv.config.EnableGzip {
		payload, err = gZipData(payload)
	}
	if err != nil {
		return err
	}

	pr, err := serv.cm.Publish(ctx, &paho.Publish{
		QoS:     qos,
		Topic:   topic,
		Payload: payload,
	})
	if err != nil {
		serv.printf("error publishing: %s\n", err)
		return err
	} else if pr != nil && pr.ReasonCode != 0 && pr.ReasonCode != 16 { // 16 = Server received message but there are no subscribers{
		serv.printf("reason code %d received\n", pr.ReasonCode)
	} else {
		serv.printf("sent topic [%s] message: %s\n", topic, payload)
	}
	return nil
}

func (serv *mqttServ) PublishViaQueue(ctx context.Context, topic string, qos byte, payload []byte) error {
	if serv.config.QueuePath == "" {
		return errors.New("no queue path set")
	}
	var err error
	if serv.config.EnableGzip {
		payload, err = gZipData(payload)
	}
	if err != nil {
		return err
	}

	err = serv.cm.PublishViaQueue(ctx, &autopaho.QueuePublish{
		Publish: &paho.Publish{
			QoS:     qos,
			Topic:   topic,
			Payload: payload,
		},
	})
	if err != nil {
		serv.printf("error publishing: %s\n", err)
		return err
	} else {
		serv.printf("(queue) sent topic [%s] message: %s\n", topic, payload)
	}
	return nil
}

func (serv *mqttServ) SetLog(l *log.Logger) {
	serv.config.Logger = l
}

func (serv *mqttServ) printf(format string, v ...interface{}) {
	if !serv.config.Debug {
		return
	}
	if serv.config.Logger == nil {
		return
	}
	serv.config.Logger.Printf(format, v...)
}

func (serv *mqttServ) println(v ...interface{}) {
	if !serv.config.Debug {
		return
	}
	if serv.config.Logger == nil {
		return
	}
	serv.config.Logger.Println(v...)
}

func gUnzipData(data []byte) (resData []byte, err error) {
	b := bytes.NewBuffer(data)

	var r io.Reader
	r, err = gzip.NewReader(b)
	if err != nil {
		return
	}

	var resB bytes.Buffer
	_, err = resB.ReadFrom(r)
	if err != nil {
		return
	}

	resData = resB.Bytes()

	return
}

func gZipData(data []byte) (compressedData []byte, err error) {
	var b bytes.Buffer
	gz := gzip.NewWriter(&b)

	_, err = gz.Write(data)
	if err != nil {
		return
	}

	if err = gz.Flush(); err != nil {
		return
	}

	if err = gz.Close(); err != nil {
		return
	}

	compressedData = b.Bytes()

	return
}
