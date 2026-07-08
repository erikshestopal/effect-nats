import { Effect, Fiber, Layer, Stream } from "effect";
import * as NatsClient from "effect-nats/NatsClient";
import * as NodeConnector from "effect-nats/NodeConnector";

const NatsLive = NatsClient.layer({ servers: "nats://127.0.0.1:4222" }).pipe(Layer.provide(NodeConnector.layer));

const encoder = new TextEncoder();

export const publishAndSubscribe = Effect.gen(function* () {
  const nats = yield* NatsClient.NatsClient;
  const fiber = yield* nats
    .subscribe("events.created")
    .pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped({ startImmediately: true }));
  yield* nats.publish("events.created", { payload: encoder.encode("hello") });
  return yield* Fiber.join(fiber);
}).pipe(Effect.scoped, Effect.provide(NatsLive));

export const requestReply = Effect.gen(function* () {
  const nats = yield* NatsClient.NatsClient;
  const response = yield* nats.request("rpc.echo", { payload: encoder.encode("ping") });
  return response.text;
}).pipe(Effect.scoped, Effect.provide(NatsLive));
