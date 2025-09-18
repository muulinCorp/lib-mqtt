package mqtt

import (
	"io"
	"time"

	"github.com/eclipse/paho.golang/autopaho/queue"
	"github.com/eclipse/paho.golang/autopaho/queue/file"
)

type ThrottledQueue struct {
	inner queue.Queue
	delay time.Duration
}

func NewThrottledQueue(dir string, delay time.Duration) (*ThrottledQueue, error) {
	q, err := file.New(dir, "queue", ".msg")
	if err != nil {
		return nil, err
	}
	return &ThrottledQueue{
		inner: q,
		delay: delay,
	}, nil
}

// autopaho.Queue interface passthrough
func (t *ThrottledQueue) Enqueue(p io.Reader) error {
	return t.inner.Enqueue(p)
}

func (t *ThrottledQueue) Wait() chan struct{} {
	return t.inner.Wait()
}

// New methods for manual control
func (t *ThrottledQueue) Peek() (queue.Entry, error) {
	time.Sleep(t.delay)
	return t.inner.Peek()
}
