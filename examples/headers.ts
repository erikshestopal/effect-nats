/**
 * Scenario: publish/request with NATS headers (nats.js core/examples/snippets/headers.ts).
 *
 * Run: `bun examples/headers.ts`
 */
import { Console, Effect, Fiber, Stream } from "effect";
import * as NatsClient from "effect-nats/NatsClient";
import * as NatsHeaders from "effect-nats/NatsHeaders";
import * as NatsMessage from "effect-nats/NatsMessage";
import { encoder, NatsLive, runMain } from "./_shared.ts";

const program = Effect.gen(function* () {
  const nats = yield* NatsClient.NatsClient;

  const sub = yield* nats
    .subscribe("demo.headers")
    .pipe(Stream.take(1), Stream.runCollect, Effect.forkScoped({ startImmediately: true }));

  yield* nats.publish("demo.headers", {
    payload: encoder.encode("payload"),
    headers: {
      ID: "msg-1",
      "X-Trace": ["a", "b"],
    },
  });

  const [message] = yield* Fiber.join(sub);
  const headers = message?.headers;

  yield* Console.log("ID", headers ? NatsHeaders.get(headers, "ID") : undefined);
  yield* Console.log("X-Trace", headers ? NatsHeaders.getAll(headers, "X-Trace") : undefined);
  yield* Console.log("record", headers ? NatsHeaders.toRecord(headers) : undefined);

  // Request carries headers the other way as well.
  yield* nats.subscribe("demo.headers.rpc").pipe(
    Stream.take(1),
    Stream.mapEffect((msg) =>
      NatsMessage.respond(msg, {
        payload: encoder.encode("ok"),
        headers: { "X-Reply": "true" },
      }),
    ),
    Stream.runDrain,
    Effect.forkScoped({ startImmediately: true }),
  );

  const response = yield* nats.request("demo.headers.rpc", {
    payload: encoder.encode("ping"),
    headers: { "X-Request": "1" },
  });

  return {
    requestId: headers ? NatsHeaders.get(headers, "ID") : undefined,
    replyHeader: NatsHeaders.get(response.headers, "X-Reply"),
  };
}).pipe(Effect.scoped, Effect.provide(NatsLive));

runMain({ label: "headers", effect: program });
