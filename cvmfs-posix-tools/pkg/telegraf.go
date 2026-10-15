package pkg

import (
	"io"
	"net"
	"time"

	"github.com/rs/zerolog/log"
)

type Telegraf struct {
	conn    *net.UDPConn
	Timeout time.Duration
}

func NewTelegraf(addr string) (*Telegraf, error) {
	if addr == "" {
		addr = DefaultTelegrafAddr
	}

	conn, err := net.Dial("udp", addr)
	if err != nil {
		return &Telegraf{}, err
	}

	return &Telegraf{
		conn:    conn.(*net.UDPConn),
		Timeout: DefaultTimeout,
	}, nil
}

func (t *Telegraf) Close() {
	if t.conn != nil {
		t.conn.Close()
		t.conn = nil
	}
}

func (t *Telegraf) Write(b []byte) (int, error) {
	if t.conn == nil {
		return -1, nil
	}

	if err := t.conn.SetDeadline(time.Now().Add(t.Timeout)); err != nil {
		return -1, err
	}

	return t.conn.Write(b)
}

func SendTelegrafStatistics(metrics, telegrafAddr string) {

	tg, err := NewTelegraf(telegrafAddr)
	if err != nil {
		log.Error().Msgf("connnecting to Telegraf: %v", err)
	}
	defer tg.Close()

	log.Debug().Str("Metrics", metrics).Str("TelegrafAddr", telegrafAddr).Msg("Sending metrics to Telegraf")
	if _, err = io.WriteString(tg, metrics); err != nil {
		log.Error().Msgf("sending metrics to Telegraf: %v", err)
	}
}
