/**
 * Scenario: continuous consume + processWith (nats.js 04/05/08 consume examples).
 *
 * Requires `nats-server -js`.
 * Run: `bun examples/jetstream-consume.ts`
 */
import { Console, Effect, Stream } from "effect";
import * as JetStream from "effect-nats/JetStream";
import * as JetStreamManager from "effect-nats/JetStreamManager";
import * as JsMessage from "effect-nats/JsMessage";
import { encoder, FullJetStreamLive, runMain } from "./_shared.ts";

const stream = "EX_JS_CONSUME";
const subject = "ex.js.consume";
const durable = "processor";

const program = Effect.gen(function* () {
  const manager = yield* JetStreamManager.JetStreamManager;
  const js = yield* JetStream.JetStream;
  const messages = yield* JsMessage.JsMessageService;

  yield* manager.streams.delete(stream).pipe(Effect.ignore);
  yield* manager.streams.add({ name: stream, subjects: [subject] });
  yield* manager.consumers.add(stream, {
    durable_name: durable,
    ack_policy: JetStreamManager.AckPolicy.Explicit,
    deliver_policy: JetStreamManager.DeliverPolicy.All,
  });

  for (let index = 0; index < 10; index++) {
    yield* js.publish(subject, { payload: encoder.encode(`order-${index}`) });
  }

  const consumer = yield* js.consumer(stream, durable);

  // processWith acks on success, naks/terms on failure — prefer this over manual ack.
  const processed = yield* consumer.consume({ maxMessages: 5 }).pipe(
    Stream.take(10),
    Stream.mapEffect((message) =>
      messages
        .processWith({
          handler: (msg) => Console.log("processing", msg.text),
        })(message)
        .pipe(Effect.as(message.text)),
    ),
    Stream.runCollect,
  );

  yield* manager.streams.delete(stream).pipe(Effect.ignore);

  const result = { processed: [...processed] };
  yield* Console.log(result);
  return result;
}).pipe(Effect.scoped, Effect.provide(FullJetStreamLive));

runMain({ label: "jetstream-consume", effect: program });
