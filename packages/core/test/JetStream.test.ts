import { assert, describe, it } from "@effect/vitest";
import { Effect, Equal, Layer, Option } from "effect";
import * as JetStream from "effect-nats/JetStream";
import * as NatsClient from "effect-nats/NatsClient";
import * as NodeConnector from "effect-nats/NodeConnector";
import * as TestNatsServer from "./utils/TestNatsServer.ts";
import { withStream } from "./utils/jsFixtures.ts";

const encoder = new TextEncoder();

const clientLayer = (options: NatsClient.Options = {}) =>
  NatsClient.layer(options).pipe(Layer.provide(NodeConnector.layer));

const jetStreamLayer = (options: NatsClient.Options = {}) =>
  JetStream.layer().pipe(Layer.provide(clientLayer(options)));

describe("JetStream", () => {
  it.effect("publishes acknowledged messages with sequence numbers", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const ack = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE6_PUB", subjects: ["phase6.pub"] });
          const js = yield* JetStream.JetStream;
          return yield* js.publish("phase6.pub", { payload: encoder.encode("one") });
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.isTrue(
        Equal.equals(
          ack,
          JetStream.PubAck.make({ stream: "PHASE6_PUB", seq: 1, duplicate: false, domain: Option.none() }),
        ),
      );
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("deduplicates repeated msgID publishes", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const duplicate = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE6_DEDUP", subjects: ["phase6.dedup"] });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase6.dedup", { msgID: "same" });
          return yield* js.publish("phase6.dedup", { msgID: "same" });
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.strictEqual(duplicate.duplicate, true);
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("maps wrong last subject sequence expectations", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE6_CAS", subjects: ["phase6.cas"] });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase6.cas");
          const error = yield* Effect.flip(js.publish("phase6.cas", { expect: { lastSubjectSequence: 0 } }));
          const ack = yield* js.publish("phase6.cas", { expect: { lastSubjectSequence: 1 } });
          return { error, ack };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.strictEqual(result.error._tag, "WrongLastSequenceError");
      assert.strictEqual(result.ack.seq, 2);
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("maps stream expectation mismatches to JetStreamApiError", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const error = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE6_EXPECT", subjects: ["phase6.expect"] });
          const js = yield* JetStream.JetStream;
          return yield* Effect.flip(js.publish("phase6.expect", { expect: { streamName: "OTHER" } }));
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.strictEqual(error._tag, "JetStreamApiError");
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("maps missing JetStream support", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const error = yield* Effect.scoped(
        Effect.gen(function* () {
          const js = yield* JetStream.JetStream;
          return yield* Effect.flip(js.publish("phase6.disabled"));
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.strictEqual(error._tag, "JetStreamNotEnabledError");
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );
});
