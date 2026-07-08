/**
 * Scenario: multi-endpoint NATS service + discovery (nats.js services examples).
 *
 * Run: `bun examples/micro.ts`
 */
import { Array as Arr, Console, Effect, Layer, Number as Num, Schema, Stream } from "effect";
import * as NatsClient from "effect-nats/NatsClient";
import * as NatsMicro from "effect-nats/NatsMicro";
import { encoder, NatsLive, runMain } from "./_shared.ts";

const Numbers = Schema.Array(Schema.Finite);
const NumbersJson = Schema.fromJsonString(Numbers);
const FiniteJson = Schema.fromJsonString(Schema.Finite);

const parseNumbers = (message: { readonly text: string }) =>
  Schema.decodeUnknownEffect(NumbersJson)(message.text).pipe(
    Effect.mapError(
      (cause) =>
        new NatsMicro.EndpointError({
          code: 400,
          description: `invalid input: ${String(cause)}`,
        }),
    ),
  );

const encodeFinite = (value: number) =>
  Schema.encodeEffect(FiniteJson)(value).pipe(
    Effect.map((json) => encoder.encode(json)),
    Effect.orDie,
  );

const CalcService = NatsMicro.layer({
  name: "calc",
  version: "1.0.0",
  description: "example calculator service",
  endpoints: {
    sum: {
      subject: "calc.sum",
      handler: (message) =>
        Effect.gen(function* () {
          const values = yield* parseNumbers(message);
          return yield* encodeFinite(Arr.reduce(values, 0, Num.sum));
        }),
    },
    max: {
      subject: "calc.max",
      handler: (message) =>
        Effect.gen(function* () {
          const values = yield* parseNumbers(message);
          return yield* encodeFinite(Arr.reduce(values, Number.NEGATIVE_INFINITY, Num.max));
        }),
    },
  },
});

const program = Effect.gen(function* () {
  const nats = yield* NatsClient.NatsClient;
  const client = yield* NatsMicro.client;

  const payload = yield* Schema.encodeEffect(NumbersJson)([1, 5, 3, 9, 2]).pipe(
    Effect.map((json) => encoder.encode(json)),
    Effect.orDie,
  );

  const sum = yield* nats.request("calc.sum", { payload });
  const max = yield* nats.request("calc.max", { payload });

  const info = yield* client.info({ name: "calc" }).pipe(Stream.take(1), Stream.runCollect);
  const stats = yield* client.stats({ name: "calc" }).pipe(Stream.take(1), Stream.runCollect);

  const result = {
    sum: sum.text,
    max: max.text,
    serviceName: info[0]?.name,
    version: info[0]?.version,
    endpoints: stats[0]?.endpoints?.map((endpoint) => endpoint.name).sort(),
  };

  yield* Console.log(result);
  return result;
}).pipe(Effect.scoped, Effect.provide(CalcService.pipe(Layer.provideMerge(NatsLive))));

runMain({ label: "micro", effect: program });
