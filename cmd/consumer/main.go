package main

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
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

	options := mqtt.NewClientOptions().
		SetClientID(clientID).
		SetConnectRetry(true).
		SetConnectRetryInterval(time.Second).
		SetAutoReconnect(true).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			logger.Warn("connection lost", "error", err)
		}).
		// Subscribing in OnConnect re-establishes subscriptions after a
		// failover to another broker, which starts a fresh session.
		SetOnConnectHandler(func(client mqtt.Client) {
			logger.Info("connected", "brokers", brokerURLs)
			subscribe := client.Subscribe("#", 1, receiveMessage(logger))
			if !subscribe.WaitTimeout(10 * time.Second) {
				logger.Warn("subscribe timed out")
			} else if err := subscribe.Error(); err != nil {
				logger.Warn("subscribe failed", "error", err)
			} else {
				logger.Info("subscribed")
			}
		})
	for _, brokerURL := range brokerURLs {
		options.AddBroker(brokerURL)
	}

	client := mqtt.NewClient(options)
	connect := client.Connect()
	for !connect.WaitTimeout(time.Second) {
		if ctx.Err() != nil {
			return nil
		}
	}
	if err := connect.Error(); err != nil {
		return fmt.Errorf("connecting to %s: %w", strings.Join(brokerURLs, ", "), err)
	}
	defer client.Disconnect(250)

	<-ctx.Done()
	logger.Info("shutting down")
	return nil
}

func receiveMessage(logger *slog.Logger) mqtt.MessageHandler {
	return func(_ mqtt.Client, msg mqtt.Message) {
		var m message
		if err := json.Unmarshal(msg.Payload(), &m); err != nil {
			logger.Info("received", "topic", msg.Topic(), "payload", string(msg.Payload()))
			return
		}
		logger.Info("received",
			"topic", msg.Topic(),
			"producer", m.Producer,
			"sequence", m.Sequence,
			"sent_at", m.SentAt.Format(time.RFC3339Nano),
			"latency", time.Since(m.SentAt).Round(time.Millisecond),
		)
	}
}

func defaultClientID() string {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Sprintf("consumer-%d", os.Getpid())
	}
	return hostname
}
