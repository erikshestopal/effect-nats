/**
 * Scenario: pull consumers via next + fetch (nats.js 02_next.js / 03_batch.js).
 *
 * Requires `nats-server -js`.
 * Run: `bun examples/jetstream-pull.ts`
 */
import { Console, Effect, Option, Stream } from "effect";
import * as JetStream from "effect-nats/JetStream";
import * as JetStreamManager from "effect-nats/JetStreamManager";
import * as JsMessage from "effect-nats/JsMessage";
import { encoder, FullJetStreamLive, runMain } from "./_shared.ts";

const stream = "EX_JS_PULL";
const subject = "ex.js.pull";
const durable = "puller";

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

  for (let index = 0; index < 5; index++) {
    yield* js.publish(subject, { payload: encoder.encode(String(index)) });
  }

  const consumer = yield* js.consumer(stream, durable);

  // next: one message or none when the pull expires.
  const one = yield* consumer.next({ expires: "2 seconds" });
  if (Option.isSome(one)) {
    yield* messages.ack(one.value);
  }

  // fetch: bounded batch iterator as a Stream.
  const batch = yield* consumer.fetch({ maxMessages: 4, expires: "2 seconds" }).pipe(
    Stream.mapEffect((message) => messages.ack(message).pipe(Effect.as(message.text))),
    Stream.runCollect,
  );

  // Empty pull after draining.
  const empty = yield* consumer.next({ expires: "500 millis" });

  yield* manager.streams.delete(stream).pipe(Effect.ignore);

  const result = {
    first: Option.map(one, (message) => message.text),
    batch: [...batch],
    emptyAfterDrain: Option.isNone(empty),
  };

  yield* Console.log(result);
  return result;
}).pipe(Effect.scoped, Effect.provide(FullJetStreamLive));

runMain({ label: "jetstream-pull", effect: program });
