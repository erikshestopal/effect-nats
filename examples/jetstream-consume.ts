import { Effect, Layer, Stream } from "effect";
import * as JetStream from "effect-nats/JetStream";
import * as JsMessage from "effect-nats/JsMessage";
import * as NatsClient from "effect-nats/NatsClient";
import * as NodeConnector from "effect-nats/NodeConnector";

const NatsLive = NatsClient.layer({ servers: "nats://127.0.0.1:4222" }).pipe(Layer.provide(NodeConnector.layer));
const JetStreamLive = JetStream.layer().pipe(Layer.provide(NatsLive));

export const consumeOrders = Effect.gen(function* () {
  const js = yield* JetStream.JetStream;
  const messages = yield* JsMessage.JsMessageService;
  const consumer = yield* js.consumer("ORDERS", "processor");

  yield* consumer.consume({ maxMessages: 10 }).pipe(
    Stream.mapEffect((message) => messages.processWith({ handler: () => Effect.void })(message)),
    Stream.runDrain,
  );
}).pipe(Effect.scoped, Effect.provide(JetStreamLive));
