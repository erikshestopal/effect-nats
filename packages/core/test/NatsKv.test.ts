import { assert, describe, it } from "@effect/vitest";
import { Effect, Fiber, Layer, Number as Num, Option, Predicate, Schema, Stream } from "effect";
import * as KeyValueStore from "effect/unstable/persistence/KeyValueStore";
import * as JetStream from "effect-nats/JetStream";
import * as NatsClient from "effect-nats/NatsClient";
import * as NatsKv from "effect-nats/NatsKv";
import * as NodeConnector from "effect-nats/NodeConnector";
import * as TestNatsServer from "./utils/TestNatsServer.ts";

const encoder = new TextEncoder();
const JsonPayload = Schema.Struct({ id: Schema.String });

const clientLayer = (options: NatsClient.Options = {}) =>
  NatsClient.layer(options).pipe(Layer.provide(NodeConnector.layer));

const jetStreamLayer = (options: NatsClient.Options = {}) =>
  JetStream.layer().pipe(Layer.provide(clientLayer(options)));

describe("NatsKv", () => {
  it.effect("creates, opens, reads missing keys as none, and reads historical revisions", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const missing = yield* Effect.flip(NatsKv.open("PHASE10_MISSING"));
          const created = yield* NatsKv.create("PHASE10_CREATE", { history: 2 });
          const opened = yield* NatsKv.open("PHASE10_CREATE");
          const empty = yield* opened.get("key");
          const revision1 = yield* created.put("key", encoder.encode("one"));
          const revision2 = yield* created.put("key", encoder.encode("two"));
          const current = yield* opened.get("key").pipe(Effect.map(Option.getOrThrow));
          const historical = yield* opened.get("key", { revision: revision1 }).pipe(Effect.map(Option.getOrThrow));
          return { missing, empty, revision1, revision2, current, historical };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.isTrue(Predicate.isTagged(result.missing, "BucketNotFoundError"));
      assert.isTrue(Option.isNone(result.empty));
      assert.strictEqual(result.revision2, Num.increment(result.revision1));
      assert.strictEqual(NatsKv.entryText(result.current), "two");
      assert.strictEqual(NatsKv.entryText(result.historical), "one");
      assert.strictEqual(result.current.operation, "PUT");
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("creates buckets with Effect-shaped options and provides a bucket layer", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const kv = yield* NatsKv.NatsKv;
          const status = yield* kv.status;
          return status;
        }).pipe(
          Effect.provide(
            NatsKv.layer("PHASE10_OPTIONS", {
              history: 4,
              ttl: "1 minute",
              maxBucketSize: 1_000_000,
              maxValueSize: 1024,
              storage: "memory",
              replicas: 1,
              description: "phase 10 options",
              compression: false,
              transformOptions: (options) => ({ ...options, metadata: { phase: "10" } }),
            }).pipe(Layer.provide(jetStreamLayer({ servers: server.url }))),
          ),
        ),
      );

      assert.strictEqual(result.history, 4);
      assert.strictEqual(result.ttl, 60_000);
      assert.strictEqual(result.max_bytes, 1_000_000);
      assert.strictEqual(result.maxValueSize, 1024);
      assert.strictEqual(result.description, "phase 10 options");
      assert.strictEqual(result.metadata?.phase, "10");
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("maps create conflicts, allows recreate after delete, and keeps update CAS typed", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const kv = yield* NatsKv.create("PHASE10_CAS", { history: 3 });
          const revision = yield* kv.create("key", encoder.encode("one"));
          const exists = yield* Effect.flip(kv.create("key", encoder.encode("two")));
          const invalid = yield* Effect.flip(kv.create("bad space", encoder.encode("bad")));
          const updated = yield* kv.update("key", encoder.encode("two"), { revision });
          const stale = yield* Effect.flip(kv.update("key", encoder.encode("stale"), { revision }));
          yield* kv.delete("key");
          const recreated = yield* kv.create("key", encoder.encode("three"));
          return { exists, invalid, stale, updated, recreated };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.isTrue(Predicate.isTagged(result.exists, "KeyExistsError"));
      assert.isTrue(Predicate.isTagged(result.invalid, "JetStreamError"));
      assert.isTrue(Predicate.isTagged(result.stale, "WrongLastSequenceError"));
      assert.isAbove(result.updated, 1);
      assert.isAbove(result.recreated, result.updated);
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("lists bounded keys, streams ordered history, and distinguishes delete from purge", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const kv = yield* NatsKv.create("PHASE10_STREAMS", { history: 5 });
          yield* kv.put("a.one", encoder.encode("one"));
          yield* kv.put("a.two", encoder.encode("two"));
          yield* kv.put("b.one", encoder.encode("three"));
          yield* kv.delete("a.one");
          const liveKeys = yield* kv.keys().pipe(Stream.runCollect);
          const filtered = yield* kv.keys("a.*").pipe(Stream.runCollect);
          const deletedHistory = yield* kv.history({ key: "a.one" }).pipe(Stream.runCollect);
          yield* kv.purge("a.two");
          const purgedHistory = yield* kv.history({ key: "a.two" }).pipe(Stream.runCollect);
          return { liveKeys: [...liveKeys].sort(), filtered: [...filtered].sort(), deletedHistory, purgedHistory };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.deepStrictEqual(result.liveKeys, ["a.two", "b.one"]);
      assert.deepStrictEqual(result.filtered, ["a.two"]);
      assert.deepStrictEqual(
        [...result.deletedHistory].map((entry) => entry.operation),
        ["PUT", "DEL"],
      );
      assert.deepStrictEqual(
        [...result.purgedHistory].map((entry) => entry.operation),
        ["PURGE"],
      );
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.live(
    "watches catch-up and live updates, supports updatesOnly, and ignores deletes",
    () =>
      Effect.gen(function* () {
        const server = yield* TestNatsServer.TestNatsServer;
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const kv = yield* NatsKv.create("PHASE10_WATCH", { history: 3 });
            yield* kv.put("key", encoder.encode("one"));
            const watchedFiber = yield* kv
              .watch({ key: "key" })
              .pipe(Stream.take(2), Stream.runCollect, Effect.forkChild);
            yield* Effect.sleep("100 millis");
            yield* kv.put("key", encoder.encode("two"));
            const watched = yield* Fiber.join(watchedFiber);
            const updatesFiber = yield* kv
              .watch({ key: "key", include: "updatesOnly", ignoreDeletes: true })
              .pipe(Stream.take(1), Stream.runCollect, Effect.forkChild);
            yield* Effect.sleep("100 millis");
            yield* kv.delete("key");
            yield* kv.put("key", encoder.encode("three"));
            const updatesOnly = yield* Fiber.join(updatesFiber);
            const resumed = yield* kv
              .watch({ key: "key", include: "allHistory", resumeFromRevision: 2 })
              .pipe(Stream.take(1), Stream.runCollect);
            const defaultWatch = yield* kv.watch().pipe(Stream.take(1), Stream.runCollect);
            const arrayKey = yield* kv
              .watch({ key: ["key"], include: "lastValue" })
              .pipe(Stream.take(1), Stream.runCollect);
            return { watched, updatesOnly, resumed, defaultWatch, arrayKey };
          }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
        );

        assert.deepStrictEqual(
          [...result.watched].map((entry) => [NatsKv.entryText(entry), entry.isUpdate]),
          [
            ["one", false],
            ["two", true],
          ],
        );
        assert.deepStrictEqual([...result.updatesOnly].map(NatsKv.entryText), ["three"]);
        assert.deepStrictEqual([...result.resumed].map(NatsKv.entryText), ["two"]);
        assert.strictEqual([...result.defaultWatch].length, 1);
        assert.strictEqual([...result.arrayKey].length, 1);
      }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
    { timeout: 10_000 },
  );

  it.effect("decodes schema JSON entries and fails malformed JSON with SchemaError", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const kv = yield* NatsKv.create("PHASE10_JSON");
          yield* kv.put("good", encoder.encode('{"id":"one"}'));
          yield* kv.put("bad", encoder.encode("nope"));
          const good = yield* kv.get("good").pipe(Effect.map(Option.getOrThrow));
          const bad = yield* kv.get("bad").pipe(Effect.map(Option.getOrThrow));
          const decoded = yield* NatsKv.entrySchemaJson(good, JsonPayload);
          const malformed = yield* Effect.flip(NatsKv.entrySchemaJson(bad, JsonPayload));
          return { decoded, malformed };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.deepStrictEqual(result.decoded, { id: "one" });
      assert.isTrue(Schema.isSchemaError(result.malformed));
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("provides the Effect KeyValueStore interface", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* KeyValueStore.KeyValueStore;
          yield* store.set("foo", "bar");
          const hasFoo = yield* store.has("foo");
          const foo = yield* store.get("foo");
          const modified = yield* store.modify("foo", (value) => `${value}bar`);
          yield* store.set("bytes", encoder.encode("bin"));
          const bytes = yield* store.getUint8Array("bytes");
          const size = yield* store.size;
          yield* store.remove("foo");
          const removed = yield* store.get("foo");
          const invalid = yield* Effect.flip(store.set("bad space", "x"));
          yield* store.clear;
          const empty = yield* store.isEmpty;
          return { hasFoo, foo, modified, bytes, size, removed, invalid, empty };
        }).pipe(
          Effect.provide(
            NatsKv.layerKeyValueStore("PHASE10_KVS").pipe(Layer.provide(jetStreamLayer({ servers: server.url }))),
          ),
        ),
      );

      assert.strictEqual(result.hasFoo, true);
      assert.strictEqual(result.foo, "bar");
      assert.strictEqual(result.modified, "barbar");
      assert.strictEqual(new TextDecoder().decode(result.bytes), "bin");
      assert.strictEqual(result.size, 2);
      assert.strictEqual(result.removed, undefined);
      assert.strictEqual(result.invalid.key, "bad space");
      assert.strictEqual(result.empty, true);
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );
});
