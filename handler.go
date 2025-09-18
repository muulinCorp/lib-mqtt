package mqtt

import (
	"fmt"
	"log"
	"strings"

	"github.com/muulinCorp/lib-mqtt/config"
	"github.com/muulinCorp/lib-mqtt/trans"

	"github.com/eclipse/paho.golang/paho"
)

// handler is a simple struct that provides a function to be called when a message is received. The message is parsed
// and the count followed by the raw message is written to the file (this makes it easier to sort the file)

type Handler interface {
	Close()
	handle(msg *paho.PublishReceived)
}

type handler struct {
	enableGzip bool
	trans      map[string]trans.Trans
	logger     *log.Logger
}

// NewHandler creates a new output handler and opens the output file (if applicable)
func NewHandler(conf *config.Config, tsmap map[string]trans.Trans) Handler {
	return &handler{
		enableGzip: conf.EnableGzip,
		trans:      tsmap,
		logger:     conf.Logger,
	}
}

// Close closes the file
func (o *handler) Close() {
	for _, t := range o.trans {
		t.Close()
	}
}

func (o *handler) sendMsg(t trans.Trans, topic string, data []byte) {
	var err error

	if o.enableGzip {
		o.println(fmt.Sprintf("before unzip data size: %s",
			formatFileSize(float64(len(data)), 1024)))

		data, err = gUnzipData(data)
		if err != nil {
			o.println(err.Error())
			return
		}
	}
	o.println(fmt.Sprintf("data size: %s",
		formatFileSize(float64(len(data)), 1024)))
	err = t.Send(topic, data)
	if err != nil {
		o.println("send fail: " + err.Error())
		o.println(string(data))
	}
}

// handle is called when a message is received
func (o *handler) handle(msg *paho.PublishReceived) {
	for k, t := range o.trans {
		if k == msg.Packet.Topic {
			o.sendMsg(t, msg.Packet.Topic, msg.Packet.Payload)
		} else if strings.Contains(k, "#") {
			prefix := strings.Split(k, "#")[0]
			if strings.HasPrefix(msg.Packet.Topic, prefix) {
				o.sendMsg(t, msg.Packet.Topic, msg.Packet.Payload)
			}
		}
	}
}

func (o *handler) println(a ...any) {
	if o.logger == nil {
		return
	}
	o.logger.Println(a...)
}

var sizes = []string{"B", "kB", "MB", "GB", "TB", "PB", "EB"}

func formatFileSize(s float64, base float64) string {
	unitsLimit := len(sizes)
	i := 0
	for s >= base && i < unitsLimit {
		s = s / base
		i++
	}

	f := "%.0f %s"
	if i > 1 {
		f = "%.2f %s"
	}

	return fmt.Sprintf(f, s, sizes[i])
}
