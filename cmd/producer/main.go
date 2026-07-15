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
)

type message struct {
	Producer string    `json:"producer"`
	Sequence uint64    `json:"sequence"`
	SentAt   time.Time `json:"sent_at"`
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(logger); err != nil {
		logger.Error("producer exited", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	brokerURL := cmp.Or(os.Getenv("BROKER"), "tcp://localhost:1883")
	topic := cmp.Or(os.Getenv("TOPIC"), "demo/messages")
	clientID := cmp.Or(os.Getenv("CLIENT_ID"), defaultClientID())
	interval, err := time.ParseDuration(cmp.Or(os.Getenv("PUBLISH_INTERVAL"), "1s"))
	if err != nil {
		return fmt.Errorf("parsing PUBLISH_INTERVAL: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	options := mqtt.NewClientOptions().
		AddBroker(brokerURL).
		SetClientID(clientID).
		SetConnectRetry(true).
		SetConnectRetryInterval(time.Second).
		SetAutoReconnect(true).
		SetOnConnectHandler(func(mqtt.Client) {
			logger.Info("connected", "broker", brokerURL)
		}).
		SetConnectionLostHandler(func(_ mqtt.Client, err error) {
			logger.Warn("connection lost", "error", err)
		})

	client := mqtt.NewClient(options)
	connect := client.Connect()
	for !connect.WaitTimeout(time.Second) {
		if ctx.Err() != nil {
			return nil
		}
	}
	if err := connect.Error(); err != nil {
		return fmt.Errorf("connecting to %s: %w", brokerURL, err)
	}
	defer client.Disconnect(250)

	logger.Info("publishing", "topic", topic, "interval", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for sequence := uint64(0); ; sequence++ {
		select {
		case <-ctx.Done():
			logger.Info("shutting down")
			return nil
		case sentAt := <-ticker.C:
			payload, err := json.Marshal(message{
				Producer: clientID,
				Sequence: sequence,
				SentAt:   sentAt.UTC(),
			})
			if err != nil {
				return fmt.Errorf("encoding message: %w", err)
			}
			publish := client.Publish(topic, 1, false, payload)
			if !publish.WaitTimeout(10 * time.Second) {
				logger.Warn("publish timed out", "topic", topic, "sequence", sequence)
			} else if err := publish.Error(); err != nil {
				logger.Warn("publish failed", "topic", topic, "sequence", sequence, "error", err)
			}
		}
	}
}

func defaultClientID() string {
	hostname, err := os.Hostname()
	if err != nil {
		return fmt.Sprintf("producer-%d", os.Getpid())
	}
	return hostname
}
