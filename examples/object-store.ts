/**
 * Scenario: ObjectStore put/get/list (nats.js obj/README.md).
 *
 * Requires `nats-server -js`.
 * Run: `bun examples/object-store.ts`
 */
import { Console, Effect, Option, Stream } from "effect";
import * as NatsObjectStore from "effect-nats/NatsObjectStore";
import { encoder, JetStreamLive, runMain } from "./_shared.ts";

const bucket = "EX_OBJ";

const program = Effect.gen(function* () {
  const store = yield* NatsObjectStore.create(bucket, {
    storage: "memory",
    description: "example object store",
  });

  const small = encoder.encode("hello object store");
  yield* store.putBlob({ name: "greeting.txt", description: "demo blob" }, small);

  // Streaming put for larger payloads (here still one chunk for clarity).
  yield* store.put({ name: "stream.bin" }, Stream.make(encoder.encode("chunk-1"), encoder.encode("chunk-2")));

  const greeting = yield* store.getBlob("greeting.txt").pipe(Effect.map(Option.getOrThrow));
  const entry = yield* store.get("stream.bin").pipe(Effect.map(Option.getOrThrow));
  const streamed = yield* entry.data.pipe(Stream.runCollect);
  const listed = yield* store.list;
  const info = yield* store.info("greeting.txt").pipe(Effect.map(Option.getOrThrow));

  yield* store.delete("stream.bin");

  const result = {
    greeting: new TextDecoder().decode(greeting),
    streamBytes: streamed.reduce((total, chunk) => total + chunk.byteLength, 0),
    names: listed.map((object) => object.name).sort(),
    size: info.size,
    digest: info.digest,
  };

  yield* Console.log(result);
  return result;
}).pipe(Effect.scoped, Effect.provide(JetStreamLive));

runMain({ label: "object-store", effect: program });
