/**
 * Scenario: JetStream publish with ack / msgID / expect (nats.js js_readme_publish_examples.js).
 *
 * Requires `nats-server -js`.
 * Run: `bun examples/jetstream-publish.ts`
 */
import { Console, Effect, Option } from "effect";
import * as JetStream from "effect-nats/JetStream";
import * as JetStreamManager from "effect-nats/JetStreamManager";
import { encoder, FullJetStreamLive, runMain } from "./_shared.ts";

const stream = "EX_JS_PUB";
const subject = "ex.js.pub";

const program = Effect.gen(function* () {
  const manager = yield* JetStreamManager.JetStreamManager;
  const js = yield* JetStream.JetStream;

  yield* manager.streams.add({ name: stream, subjects: [`${subject}.*`] }).pipe(Effect.ignore);
  yield* manager.streams.purge(stream).pipe(Effect.ignore);

  const first = yield* js.publish(`${subject}.a`, { payload: encoder.encode("one") });
  yield* Console.log("stored", { stream: first.stream, seq: first.seq, duplicate: first.duplicate });

  // Dedup window: same msgID is acknowledged as a duplicate.
  const withId = yield* js.publish(`${subject}.a`, {
    payload: encoder.encode("two"),
    msgID: "demo-id-1",
  });
  const dup = yield* js.publish(`${subject}.a`, {
    payload: encoder.encode("two-again"),
    msgID: "demo-id-1",
  });

  // Optimistic concurrency: require previous sequence.
  const next = yield* js.publish(`${subject}.b`, {
    payload: encoder.encode("three"),
    expect: { lastSequence: withId.seq },
  });

  yield* manager.streams.delete(stream).pipe(Effect.ignore);

  return {
    firstSeq: first.seq,
    msgIdSeq: withId.seq,
    duplicate: dup.duplicate,
    nextSeq: next.seq,
    domain: Option.getOrUndefined(next.domain),
  };
}).pipe(Effect.scoped, Effect.provide(FullJetStreamLive));

runMain({ label: "jetstream-publish", effect: program });
