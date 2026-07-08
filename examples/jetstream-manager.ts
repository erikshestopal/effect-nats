/**
 * Scenario: JetStream manager stream/consumer CRUD (nats.js jsm_readme_jsm_example.js).
 *
 * Requires `nats-server -js`.
 * Run: `bun examples/jetstream-manager.ts`
 */
import { Console, Effect, Stream } from "effect";
import * as JetStream from "effect-nats/JetStream";
import * as JetStreamManager from "effect-nats/JetStreamManager";
import { encoder, FullJetStreamLive, runMain } from "./_shared.ts";

const stream = "EX_JSM";
const subject = "ex.jsm.events";

const program = Effect.gen(function* () {
  const manager = yield* JetStreamManager.JetStreamManager;
  const js = yield* JetStream.JetStream;

  yield* manager.streams.delete(stream).pipe(Effect.ignore);

  const added = yield* manager.streams.add({
    name: stream,
    subjects: [subject],
    max_age: "1 hour",
  });

  yield* js.publish(subject, { payload: encoder.encode("one") });
  yield* js.publish(subject, { payload: encoder.encode("two") });

  const info = yield* manager.streams.info(stream);
  yield* manager.streams.update(stream, { max_age: "2 hours" });

  const consumer = yield* manager.consumers.add(stream, {
    durable_name: "worker",
    ack_policy: JetStreamManager.AckPolicy.Explicit,
    deliver_policy: JetStreamManager.DeliverPolicy.All,
    ack_wait: "30 seconds",
  });

  const streamNames = yield* manager.streams.names().pipe(Stream.runCollect);
  const consumers = yield* manager.consumers.list(stream).pipe(Stream.runCollect);

  const purged = yield* manager.streams.purge(stream);
  yield* manager.consumers.delete(stream, "worker");
  yield* manager.streams.delete(stream);

  const result = {
    stream: added.config.name,
    messagesBeforePurge: info.state.messages,
    consumer: consumer.name,
    knownStreams: [...streamNames].filter((name) => name.startsWith("EX_")),
    consumerCount: consumers.length,
    purged: purged.purged,
  };

  yield* Console.log(result);
  return result;
}).pipe(Effect.scoped, Effect.provide(FullJetStreamLive));

runMain({ label: "jetstream-manager", effect: program });
