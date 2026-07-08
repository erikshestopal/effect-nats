import { Effect, Layer } from "effect";
import * as NatsClient from "effect-nats/NatsClient";
import * as NatsMicro from "effect-nats/NatsMicro";
import * as NodeConnector from "effect-nats/NodeConnector";

const NatsLive = NatsClient.layer({ servers: "nats://127.0.0.1:4222" }).pipe(Layer.provide(NodeConnector.layer));

export const EchoService = NatsMicro.layer({
  name: "echo",
  version: "1.0.0",
  endpoints: {
    echo: {
      handler: (message) => Effect.succeed(message.payload),
    },
  },
}).pipe(Layer.provide(NatsLive));
