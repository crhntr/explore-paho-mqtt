# Explore Using MQTT with Go

Two Mosquitto brokers, four producers (two per broker), and one consumer that
registers both brokers with `AddBroker`. Everything runs in docker-compose;
the Go services run `go run ./cmd/...` in a `golang` image with the source
mounted as a volume (no Dockerfiles).

```sh
docker compose up -d
docker compose logs -f consumer
```

## Configuration

Producer (`cmd/producer`):

| Variable           | Default                | Meaning                                  |
|--------------------|------------------------|------------------------------------------|
| `BROKER`           | `tcp://localhost:1883` | Broker URL to publish to                 |
| `TOPIC`            | `demo/messages`        | Topic to publish to                      |
| `PUBLISH_INTERVAL` | `1s`                   | Time between messages (`time.Duration`)  |
| `CLIENT_ID`        | hostname               | MQTT client id                           |

Consumer (`cmd/consumer`):

| Variable        | Default  | Meaning                                              |
|-----------------|----------|------------------------------------------------------|
| `CONFIGURATION` | required | JSON array of broker URLs, one `AddBroker` per entry |
| `CLIENT_ID`     | hostname | MQTT client id                                       |

Messages are JSON: `{"producer": ..., "sequence": ..., "sent_at": ...}`. The
consumer subscribes to `#` and logs each message with its end-to-end latency.

## Finding: `AddBroker` is a failover list, not a fan-in

paho.mqtt.golang connects to **one** broker at a time. The `AddBroker` list is
tried in order when connecting or reconnecting — it does not open simultaneous
connections. So the consumer only receives messages from the producers attached
to whichever broker it is currently connected to (normally `broker-01`, the
first in the list).

Watch the failover:

```sh
docker compose stop broker-01
docker compose logs -f consumer
```

The consumer logs `connection lost`, reconnects to `broker-02`, resubscribes
(the subscription is made in the `OnConnect` handler so it survives failover),
and starts receiving from `producer-02-01` and `producer-02-02` instead.

To consume from all brokers at once you need one client per broker (or a
broker-side bridge between the Mosquitto instances).

## Package `pool`: consuming from all brokers

`pool.Proxy` manages a dynamic set of MQTT clients keyed by client id. The
consumer uses it to hold one connection per broker, so it receives from all
brokers simultaneously — each log line's `via` attribute shows which pooled
connection delivered the message.

- `New(ctx, handlers, options...)` — caller-supplied handlers use unchanged
  paho signatures; inside a handler, `client.OptionsReader().ClientID()`
  identifies the connection that fired.
- `Add(ctx, options)` — connects a client keyed by `options.ClientID`,
  waiting until connected or ctx ends. Adding an id that already exists shuts
  down the previous client and replaces it.
- `Remove(id)` / `Close()` — disconnect one / all.
- `Clients() iter.Seq2[string, mqtt.Client]` — snapshot iteration.
- `Default()` — preconfigured `*mqtt.ClientOptions` (proxy handlers, connect
  retry, auto reconnect) to override and pass back to `Add`.

Tests use counterfeiter fakes generated into `internal/fake`
(`go generate ./pool`).
