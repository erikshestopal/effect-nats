import { assert, describe, it } from "@effect/vitest";
import { DateTime, Effect, Layer, Option, Predicate, Stream } from "effect";
import * as JetStream from "effect-nats/JetStream";
import * as JetStreamManager from "effect-nats/JetStreamManager";
import * as NatsClient from "effect-nats/NatsClient";
import * as NodeConnector from "effect-nats/NodeConnector";
import * as TestNatsServer from "./utils/TestNatsServer.ts";

const encoder = new TextEncoder();

const clientLayer = (options: NatsClient.Options = {}) =>
  NatsClient.layer(options).pipe(Layer.provide(NodeConnector.layer));

const managerLayer = (options: NatsClient.Options = {}) =>
  JetStreamManager.layer().pipe(Layer.provide(clientLayer(options)));

const jetStreamLayer = (options: NatsClient.Options = {}) =>
  JetStream.layer().pipe(Layer.provide(clientLayer(options)));

describe("JetStreamManager", () => {
  it.effect("performs stream CRUD and converts duration config fields", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const manager = yield* JetStreamManager.JetStreamManager;
          const added = yield* manager.streams.add({
            name: "PHASE9_CRUD",
            subjects: ["phase9.crud"],
            max_age: "1 hour",
          });
          const info = yield* manager.streams.info("PHASE9_CRUD");
          const updated = yield* manager.streams.update("PHASE9_CRUD", { max_age: "2 hours" });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase9.crud", { payload: encoder.encode("one") });
          const beforePurge = yield* manager.streams.info("PHASE9_CRUD");
          const purged = yield* manager.streams.purge("PHASE9_CRUD");
          const afterPurge = yield* manager.streams.info("PHASE9_CRUD");
          yield* manager.streams.delete("PHASE9_CRUD");
          const missing = yield* Effect.flip(manager.streams.info("PHASE9_CRUD"));
          return { added, info, updated, beforePurge, purged, afterPurge, missing };
        }).pipe(
          Effect.provide(Layer.merge(managerLayer({ servers: server.url }), jetStreamLayer({ servers: server.url }))),
        ),
      );

      assert.strictEqual(result.added.config.max_age, 3_600_000_000_000);
      assert.strictEqual(result.info.config.name, "PHASE9_CRUD");
      assert.strictEqual(result.updated.config.max_age, 7_200_000_000_000);
      assert.strictEqual(result.beforePurge.state.messages, 1);
      assert.strictEqual(result.purged.purged, 1);
      assert.strictEqual(result.afterPurge.state.messages, 0);
      assert.isTrue(Predicate.isTagged(result.missing, "StreamNotFoundError"));
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("performs consumer CRUD and converts duration config fields", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const manager = yield* JetStreamManager.JetStreamManager;
          yield* manager.streams.add({ name: "PHASE9_CONSUMER", subjects: ["phase9.consumer"] });
          const added = yield* manager.consumers.add("PHASE9_CONSUMER", {
            durable_name: "durable",
            ack_policy: JetStreamManager.AckPolicy.Explicit,
            deliver_policy: JetStreamManager.DeliverPolicy.All,
            ack_wait: "2 seconds",
          });
          const info = yield* manager.consumers.info("PHASE9_CONSUMER", "durable");
          const updated = yield* manager.consumers.update("PHASE9_CONSUMER", "durable", {
            ack_wait: "3 seconds",
          });
          yield* manager.consumers.delete("PHASE9_CONSUMER", "durable");
          const missing = yield* Effect.flip(manager.consumers.info("PHASE9_CONSUMER", "durable"));
          yield* manager.streams.delete("PHASE9_CONSUMER");
          return { added, info, updated, missing };
        }).pipe(Effect.provide(managerLayer({ servers: server.url }))),
      );

      assert.strictEqual(result.added.config.ack_wait, 2_000_000_000);
      assert.strictEqual(result.info.name, "durable");
      assert.strictEqual(result.updated.config.ack_wait, 3_000_000_000);
      assert.isTrue(Predicate.isTagged(result.missing, "ConsumerNotFoundError"));
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("exposes stream and consumer listers as Streams", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const manager = yield* JetStreamManager.JetStreamManager;
          for (let index = 0; index < 7; index++) {
            yield* manager.streams.add({ name: `PHASE9_LIST_${index}`, subjects: [`phase9.list.${index}`] });
            yield* manager.consumers.add(`PHASE9_LIST_${index}`, {
              durable_name: "durable",
              ack_policy: JetStreamManager.AckPolicy.Explicit,
              deliver_policy: JetStreamManager.DeliverPolicy.All,
            });
          }
          const streamNames = yield* manager.streams.names("phase9.list.*").pipe(Stream.runCollect);
          const streams = yield* manager.streams.list("phase9.list.*").pipe(Stream.runCollect);
          const consumers = yield* manager.consumers.list("PHASE9_LIST_0").pipe(Stream.runCollect);
          for (let index = 0; index < 7; index++) {
            yield* manager.consumers.delete(`PHASE9_LIST_${index}`, "durable");
            yield* manager.streams.delete(`PHASE9_LIST_${index}`);
          }
          return { streamNames, streams, consumers };
        }).pipe(Effect.provide(managerLayer({ servers: server.url }))),
      );

      assert.sameMembers(
        [...result.streamNames],
        [
          "PHASE9_LIST_0",
          "PHASE9_LIST_1",
          "PHASE9_LIST_2",
          "PHASE9_LIST_3",
          "PHASE9_LIST_4",
          "PHASE9_LIST_5",
          "PHASE9_LIST_6",
        ],
      );
      assert.sameMembers(
        [...result.streams].map((stream) => stream.config.name),
        [
          "PHASE9_LIST_0",
          "PHASE9_LIST_1",
          "PHASE9_LIST_2",
          "PHASE9_LIST_3",
          "PHASE9_LIST_4",
          "PHASE9_LIST_5",
          "PHASE9_LIST_6",
        ],
      );
      assert.deepStrictEqual(
        [...result.consumers].map((consumer) => consumer.name),
        ["durable"],
      );
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.live(
    "pauses and resumes consumers",
    () =>
      Effect.gen(function* () {
        const server = yield* TestNatsServer.TestNatsServer;
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const manager = yield* JetStreamManager.JetStreamManager;
            yield* manager.streams.add({ name: "PHASE9_PAUSE", subjects: ["phase9.pause"] });
            yield* manager.consumers.add("PHASE9_PAUSE", {
              durable_name: "durable",
              ack_policy: JetStreamManager.AckPolicy.Explicit,
              deliver_policy: JetStreamManager.DeliverPolicy.All,
            });
            const until = DateTime.add(yield* DateTime.now, { minutes: 5 });
            const paused = yield* manager.consumers.pause("PHASE9_PAUSE", "durable", { until });
            const js = yield* JetStream.JetStream;
            yield* js.publish("phase9.pause", { payload: encoder.encode("one") });
            const consumer = yield* js.consumer("PHASE9_PAUSE", "durable");
            const duringPause = yield* consumer.next({ expires: "1 second" });
            const resumed = yield* manager.consumers.resume("PHASE9_PAUSE", "durable");
            const afterResume = yield* consumer.next({ expires: "1 second" });
            yield* manager.consumers.delete("PHASE9_PAUSE", "durable");
            yield* manager.streams.delete("PHASE9_PAUSE");
            return { paused, duringPause, resumed, afterResume };
          }).pipe(
            Effect.provide(Layer.merge(managerLayer({ servers: server.url }), jetStreamLayer({ servers: server.url }))),
          ),
        );

        assert.strictEqual(result.paused.paused, true);
        assert.isTrue(Option.isNone(result.duringPause));
        assert.strictEqual(result.resumed.paused, false);
        assert.strictEqual(Option.getOrThrow(result.afterResume).text, "one");
      }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
    { timeout: 10_000 },
  );

  it.effect("maps missing JetStream manager support", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const error = yield* JetStreamManager.make().pipe(
        Effect.provide(clientLayer({ servers: server.url })),
        Effect.flip,
      );

      assert.isTrue(Predicate.isTagged(error, "JetStreamNotEnabledError"));
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );
});
