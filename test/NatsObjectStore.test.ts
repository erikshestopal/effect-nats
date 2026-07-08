import { assert, describe, it } from "@effect/vitest";
import { Effect, Fiber, Layer, Option, Stream } from "effect";
import * as JetStream from "effect-nats/JetStream";
import * as NatsClient from "effect-nats/NatsClient";
import * as NatsObjectStore from "effect-nats/NatsObjectStore";
import * as NodeConnector from "effect-nats/NodeConnector";
import * as TestNatsServer from "./utils/TestNatsServer.ts";

const clientLayer = (options: NatsClient.Options = {}) =>
  NatsClient.layer(options).pipe(Layer.provide(NodeConnector.layer));

const jetStreamLayer = (options: NatsClient.Options = {}) =>
  JetStream.layer().pipe(Layer.provide(clientLayer(options)));

const bytes = (size: number) => Uint8Array.from({ length: size }, (_value, index) => index % 251);

const collectBytes = <E>(stream: Stream.Stream<Uint8Array, E>) =>
  stream.pipe(
    Stream.runCollect,
    Effect.map((chunks) => Buffer.concat([...chunks]).valueOf()),
  );

describe("NatsObjectStore", () => {
  it.effect("puts and gets multi-chunk streams", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const data = bytes(1024 * 1024);
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* NatsObjectStore.create("PHASE11_STREAM");
          const put = yield* store.put({ name: "big", maxChunkSize: 64 * 1024 }, Stream.make(data));
          const opened = yield* NatsObjectStore.open("PHASE11_STREAM");
          const entry = yield* store.get("big").pipe(Effect.map(Option.getOrThrow));
          const roundTrip = yield* collectBytes(entry.data);
          const openedInfo = yield* opened.info("big").pipe(Effect.map(Option.getOrThrow));
          const js = yield* JetStream.JetStream;
          yield* js.publish(`$O.PHASE11_STREAM.C.${put.nuid}`, { payload: bytes(4) });
          const corruptEntry = yield* store.get("big").pipe(Effect.map(Option.getOrThrow));
          const digestFailure = yield* Effect.flip(collectBytes(corruptEntry.data));
          return { put, entry, roundTrip, openedInfo, digestFailure };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.isAbove(result.put.chunks, 1);
      assert.strictEqual(result.entry.info.name, "big");
      assert.strictEqual(result.openedInfo.name, "big");
      assert.deepStrictEqual(result.roundTrip, data);
      assert.strictEqual(result.digestFailure._tag, "DigestMismatchError");
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("supports blob helpers, info, list, delete, status, and layer", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const store = yield* NatsObjectStore.NatsObjectStore;
          const missing = yield* store.getBlob("missing");
          const put = yield* store.putBlob(
            {
              name: "blob",
              description: "blob desc",
              headers: { phase: "11" },
              metadata: { type: "test" },
            },
            bytes(16),
          );
          const blob = yield* store.getBlob("blob").pipe(Effect.map(Option.getOrThrow));
          const info = yield* store.info("blob").pipe(Effect.map(Option.getOrThrow));
          const history = yield* store.watch({ includeHistory: true }).pipe(Stream.take(1), Stream.runCollect);
          const list = yield* store.list;
          yield* store.delete("blob");
          const deleted = yield* store.info("blob");
          const status = yield* store.status;
          return { missing, put, blob, info, history, list, deleted, status };
        }).pipe(
          Effect.provide(
            NatsObjectStore.layer("PHASE11_BLOB", {
              description: "bucket desc",
              ttl: "1 hour",
              storage: "memory",
              replicas: 1,
              maxBucketSize: 1024 * 1024,
              metadata: { phase: "11" },
              compression: true,
            }).pipe(Layer.provide(jetStreamLayer({ servers: server.url }))),
          ),
        ),
      );

      assert.isTrue(Option.isNone(result.missing));
      assert.strictEqual(result.put.name, "blob");
      assert.deepStrictEqual(result.blob, bytes(16));
      assert.strictEqual(result.info.description, "blob desc");
      assert.strictEqual(result.info.metadata?.type, "test");
      assert.deepStrictEqual(
        [...result.history].map((info) => info.name),
        ["blob"],
      );
      assert.deepStrictEqual(
        result.list.map((info) => info.name),
        ["blob"],
      );
      assert.isTrue(Option.isNone(result.deleted));
      assert.strictEqual(result.status.bucket, "PHASE11_BLOB");
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.live(
    "watches puts and deletes and fails puts with failing input streams",
    () =>
      Effect.gen(function* () {
        const server = yield* TestNatsServer.TestNatsServer;
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const store = yield* NatsObjectStore.create("PHASE11_WATCH");
            const watchedFiber = yield* store.watch().pipe(Stream.take(2), Stream.runCollect, Effect.forkChild);
            yield* Effect.sleep("100 millis");
            yield* store.putBlob({ name: "one" }, bytes(8));
            yield* store.delete("one");
            const watched = yield* Fiber.join(watchedFiber);
            const failed = yield* Effect.flip(store.put({ name: "bad" }, Stream.fromEffect(Effect.fail("boom"))));
            return { watched, failed };
          }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
        );

        assert.deepStrictEqual(
          [...result.watched].map((info) => [info.name, info.deleted]),
          [
            ["one", false],
            ["one", true],
          ],
        );
        assert.strictEqual(result.failed._tag, "ObjectStoreError");
      }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
    { timeout: 10_000 },
  );

  it.effect("maps missing buckets and sealed stores", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const missingBucket = yield* Effect.flip(NatsObjectStore.open("PHASE11_MISSING"));
          const store = yield* NatsObjectStore.create("PHASE11_SEAL");
          yield* store.seal;
          const failedPut = yield* Effect.flip(store.putBlob({ name: "sealed" }, bytes(1)));
          return { missingBucket, failedPut };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.strictEqual(result.missingBucket._tag, "BucketNotFoundError");
      assert.strictEqual(result.failedPut._tag, "ObjectStoreError");
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );
});
