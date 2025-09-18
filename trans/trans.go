package trans

type Trans interface {
	Send(string, []byte) error
	Close()
}

func NewSimpleTrans(myFunc func(string, []byte) error) Trans {
	return &simpleTrans{sendFunc: myFunc}
}

type simpleTrans struct {
	sendFunc func(string, []byte) error
}

func (t *simpleTrans) Send(topic string, data []byte) error {
	return t.sendFunc(topic, data)
}

func (t *simpleTrans) Close() {
}
