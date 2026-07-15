package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"

	"github.com/crhntr/explore-paho-mqtt/pool"
)

type message struct {
	Producer string    `json:"producer"`
	Sequence uint64    `json:"sequence"`
	SentAt   time.Time `json:"sent_at"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("consumer exited", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	var brokerURLs []string
	if err := json.Unmarshal([]byte(os.Getenv("CONFIGURATION")), &brokerURLs); err != nil {
		return fmt.Errorf("parsing CONFIGURATION: %w", err)
	}
	clientID := cmp.Or(os.Getenv("CLIENT_ID"), defaultClientID())

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	proxy, err := pool.New(ctx, pool.Handlers{
		OnConnect: func(client mqtt.Client) {
			id := connectionID(client)
			logger.Info("connected", "client_id", id)
			subscribe := client.Subscribe("#", 1, receiveMessage(logger))
			if !subscribe.WaitTimeout(10 * time.Second) {
				logger.Warn("subscribe timed out", "client_id", id)
			} else if err := subscribe.Error(); err != nil {
				logger.Warn("subscribe failed", "client_id", id, "error", err)
			} else {
				logger.Info("subscribed", "client_id", id)
			}
		},
		ConnectionLost: func(client mqtt.Client, err error) {
			logger.Warn("connection lost", "client_id", connectionID(client), "error", err)
		},
		Reconnecting: func(_ mqtt.Client, options *mqtt.ClientOptions) {
			logger.Info("reconnecting", "client_id", options.ClientID)
		},
	})
	if err != nil {
		return fmt.Errorf("creating connection pool: %w", err)
	}
	defer proxy.Close()

	// One pooled client per broker so the consumer receives from all brokers
	// simultaneously — paho itself only holds one connection per client.
	for i, brokerURL := range brokerURLs {
		options := proxy.Default().
			AddBroker(brokerURL).
			SetClientID(fmt.Sprintf("%s-%02d", clientID, i))
		if err := proxy.Add(ctx, options); err != nil {
			return fmt.Errorf("adding broker %s: %w", brokerURL, err)
		}
	}

	<-ctx.Done()
	logger.Info("shutting down")
	return nil
}

func receiveMessage(logger *slog.Logger) mqtt.MessageHandler {
	return func(client mqtt.Client, msg mqtt.Message) {
		var m message
		if err := json.Unmarshal(msg.Payload(), &m); err != nil {
			logger.Info("received", "topic", msg.Topic(), "payload", string(msg.Payload()))
			return
		}
		logger.Info("received",
			"via", connectionID(client),
			"topic", msg.Topic(),
			"producer", m.Producer,
			"sequence", m.Sequence,
			"latency", time.Since(m.SentAt).Round(time.Millisecond),
		)
	}
}

// connectionID identifies which pooled connection an event came from.
// ClientOptionsReader methods use a pointer receiver, so the reader needs a
// variable before ClientID can be called.
func connectionID(client mqtt.Client) string {
	reader := client.OptionsReader()
	return reader.ClientID()
}

func defaultClientID() string {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Sprintf("consumer-%d", os.Getpid())
	}
	return hostname
}
