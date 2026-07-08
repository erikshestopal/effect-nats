import { assert, describe, it } from "@effect/vitest";
import { expectTypeOf } from "expect-type";
import { Clock, Context, Effect, Exit, Fiber, Layer, Option, Scope, Stream } from "effect";
import * as NatsClient from "effect-nats/NatsClient";
import * as NatsError from "effect-nats/NatsError";
import * as NatsHeaders from "effect-nats/NatsHeaders";
import * as NatsMessage from "effect-nats/NatsMessage";
import * as NatsMicro from "effect-nats/NatsMicro";
import * as NodeConnector from "effect-nats/NodeConnector";
import * as TestNatsServer from "./utils/TestNatsServer.ts";

class Prefix extends Context.Service<Prefix, { readonly value: string }>()("test/Prefix") {}

const text = new TextEncoder();

const clientLayer = (options: NatsClient.Options = {}) =>
  NatsClient.layer(options).pipe(Layer.provide(NodeConnector.layer));

const serviceStack = <R>(
  serverUrl: string,
  service: Layer.Layer<never, NatsError.NatsError, NatsClient.NatsClient | R>,
) => {
  const client = clientLayer({ servers: serverUrl });
  return Layer.merge(client, service.pipe(Layer.provide(client)));
};

const requestText = (subject: string, payload = "") =>
  Effect.flatMap(NatsClient.NatsClient, (nats) =>
    nats.request(subject, { payload: text.encode(payload) }).pipe(Effect.map((message) => message.text)),
  );

const collect = <A, E>(stream: Stream.Stream<A, E>) =>
  stream.pipe(
    Stream.runCollect,
    Effect.map((chunk) => [...chunk]),
  );

describe("NatsMicro", () => {
  it.effect("returns running service metadata from make", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const running = yield* NatsMicro.make({
            name: "svc-running",
            version: "1.0.0",
            endpoints: { direct: { handler: (msg) => Effect.succeed(msg.payload) } },
          });
          yield* running.stopped.pipe(Effect.forkChild({ startImmediately: true }));
          const response = yield* requestText("direct", "ok");
          return { name: running.info.name, response };
        }).pipe(Effect.provide(clientLayer({ servers: server.url }))),
      );

      assert.deepStrictEqual(result, { name: "svc-running", response: "ok" });
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("serves requests, supports dependencies, manual responses, and custom subjects", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const options = {
        name: "svc-main",
        version: "1.0.0",
        description: "main service",
        metadata: { phase: "12" },
        queue: "svc-main-q",
        endpoints: {
          echo: { metadata: { kind: "echo" }, handler: (msg) => Effect.succeed(msg.payload) },
          prefixed: {
            subject: "acct.v1.check",
            queue: "acct-q",
            handler: (msg) => Effect.map(Prefix, (prefix) => text.encode(`${prefix.value}:${msg.text}`)),
          },
          manual: {
            handler: (msg) =>
              NatsMessage.respond(msg, { payload: text.encode("manual") }).pipe(Effect.orDie, Effect.as(undefined)),
          },
        },
      } satisfies NatsMicro.ServiceOptions<Prefix>;

      expectTypeOf(NatsMicro.layer(options)).toMatchTypeOf<
        Layer.Layer<never, NatsError.NatsError, NatsClient.NatsClient | Prefix>
      >();

      const result = yield* Effect.scoped(
        Effect.all({
          echo: requestText("echo", "hello"),
          prefixed: requestText("acct.v1.check", "ok"),
          manual: requestText("manual"),
        }).pipe(
          Effect.provide(
            serviceStack(
              server.url,
              NatsMicro.layer(options).pipe(Layer.provide(Layer.succeed(Prefix, Prefix.of({ value: "dep" })))),
            ),
          ),
        ),
      );

      assert.deepStrictEqual(result, { echo: "hello", prefixed: "dep:ok", manual: "manual" });
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("maps endpoint errors and defects to service error headers and keeps serving", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          const nats = yield* NatsClient.NatsClient;
          const typed = yield* nats.request("typed");
          const defect = yield* nats.request("defect");
          const ok = yield* requestText("ok", "still");
          return { typed, defect, ok };
        }).pipe(
          Effect.provide(
            serviceStack(
              server.url,
              NatsMicro.layer({
                name: "svc-errors",
                version: "1.0.0",
                endpoints: {
                  typed: {
                    handler: () => Effect.fail(new NatsMicro.EndpointError({ code: 418, description: "teapot" })),
                  },
                  defect: { handler: () => Effect.die("boom") },
                  ok: { handler: (msg) => Effect.succeed(msg.payload) },
                },
              }),
            ),
          ),
        ),
      );

      assert.strictEqual(
        NatsHeaders.get(result.typed.headers, NatsMicro.ServiceErrorCodeHeader).pipe(Option.getOrThrow),
        "418",
      );
      assert.strictEqual(
        NatsHeaders.get(result.typed.headers, NatsMicro.ServiceErrorHeader).pipe(Option.getOrThrow),
        "teapot",
      );
      assert.strictEqual(
        NatsHeaders.get(result.defect.headers, NatsMicro.ServiceErrorCodeHeader).pipe(Option.getOrThrow),
        "500",
      );
      assert.strictEqual(
        NatsHeaders.get(result.defect.headers, NatsMicro.ServiceErrorHeader).pipe(Option.getOrThrow),
        "internal error",
      );
      assert.strictEqual(result.ok, "still");
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.live("handles endpoint concurrency", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const elapsed = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* NatsClient.NatsClient.pipe(Effect.flatMap((nats) => nats.flush));
          const start = yield* Clock.currentTimeMillis;
          const fibers = yield* Effect.forEach(
            Array.from({ length: 8 }, () => requestText("slow")),
            (effect) => Effect.forkChild(effect, { startImmediately: true }),
          );
          yield* Effect.forEach(fibers, Fiber.join);
          const end = yield* Clock.currentTimeMillis;
          return end - start;
        }).pipe(
          Effect.provide(
            serviceStack(
              server.url,
              NatsMicro.layer({
                name: "svc-concurrency",
                version: "1.0.0",
                endpoints: {
                  slow: {
                    concurrency: 4,
                    handler: () => Effect.sleep("100 millis").pipe(Effect.as(text.encode("ok"))),
                  },
                },
              }),
            ),
          ),
        ),
      );

      assert.isBelow(elapsed, 650);
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("discovers services and reports stats", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* requestText("echo", "one");
          yield* NatsClient.NatsClient.pipe(Effect.flatMap((nats) => nats.request("bad")));
          const micro = yield* NatsMicro.client;
          const pings = yield* collect(micro.ping({ name: "svc-discovery" }));
          const infos = yield* collect(micro.info({ name: "svc-discovery" }));
          const stats = yield* collect(micro.stats({ name: "svc-discovery" }));
          return { pings, infos, stats };
        }).pipe(
          Effect.provide(
            serviceStack(
              server.url,
              NatsMicro.layer({
                name: "svc-discovery",
                version: "1.2.3",
                endpoints: {
                  echo: { handler: (msg) => Effect.succeed(msg.payload) },
                  bad: { handler: () => Effect.fail(new NatsMicro.EndpointError({ code: 400, description: "bad" })) },
                },
              }),
            ),
          ),
        ),
      );

      assert.strictEqual(result.pings[0]?.name, "svc-discovery");
      assert.strictEqual(result.pings[0]?.version, "1.2.3");
      assert.strictEqual(result.infos[0]?.endpoints.length, 2);
      assert.isAtLeast(result.stats[0]?.endpoints?.[0]?.num_requests ?? 0, 1);
      assert.isAtLeast(result.stats[0]?.endpoints?.find((endpoint) => endpoint.name === "bad")?.num_errors ?? 0, 1);
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("scales queue groups with one response per request", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const responses = yield* Effect.scoped(
        Effect.forEach(
          Array.from({ length: 8 }, () => requestText("shared")),
          (effect) => effect,
        ).pipe(
          Effect.provide(
            serviceStack(
              server.url,
              NatsMicro.layer({
                name: "svc-shared",
                version: "1.0.0",
                endpoints: { shared: { handler: () => Effect.succeed(text.encode("one")) } },
              }),
            ).pipe(
              Layer.merge(
                NatsMicro.layer({
                  name: "svc-shared",
                  version: "1.0.0",
                  endpoints: { shared: { handler: () => Effect.succeed(text.encode("two")) } },
                }).pipe(Layer.provide(clientLayer({ servers: server.url }))),
              ),
            ),
          ),
        ),
      );

      assert.strictEqual(responses.length, 8);
      assert.isTrue(responses.every((response) => response === "one" || response === "two"));
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("stops discovery responses when the layer scope closes", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const scope = yield* Scope.make();
      const layer = serviceStack(
        server.url,
        NatsMicro.layer({
          name: "svc-scoped",
          version: "1.0.0",
          endpoints: { echo: { handler: (msg) => Effect.succeed(msg.payload) } },
        }),
      );
      const context = yield* Layer.buildWithScope(layer, scope);
      const micro = yield* Effect.provide(NatsMicro.client, context);
      const before = yield* collect(micro.ping({ name: "svc-scoped" }));
      yield* Scope.close(scope, Exit.void);
      const after = yield* Effect.scoped(
        Effect.flatMap(NatsClient.NatsClient, (nats) =>
          collect(nats.requestMany("$SRV.PING.svc-scoped", { maxWait: "100 millis" })),
        ).pipe(
          Effect.catchTag("NoRespondersError", () => Effect.succeed([])),
          Effect.provide(clientLayer({ servers: server.url })),
        ),
      );

      assert.strictEqual(before.length, 1);
      assert.deepStrictEqual(after, []);
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );
});
