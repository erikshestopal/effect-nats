import { assert, describe, it } from "@effect/vitest";
import { Clock, Config, ConfigProvider, Deferred, Duration, Effect, Layer, Option, Predicate, Redacted } from "effect";
import {
  AuthorizationError,
  ClosedConnectionError,
  DrainingConnectionError,
  InvalidSubjectError,
  NoRespondersError,
  ProtocolError,
  TimeoutError,
  UserAuthenticationExpiredError,
} from "@nats-io/nats-core";
import * as NatsClient from "effect-nats/NatsClient";
import * as NatsError from "effect-nats/NatsError";
import * as NatsHeaders from "effect-nats/NatsHeaders";
import * as NatsMessage from "effect-nats/NatsMessage";
import * as NatsConnector from "effect-nats/NatsConnector";
import * as NodeConnector from "effect-nats/NodeConnector";
import * as Errors from "../src/internal/errors.ts";
import * as Options from "../src/internal/options.ts";
import * as TestNatsServer from "./utils/TestNatsServer.ts";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

const tcpClientLayer = (options: NatsClient.Options = {}) =>
  NatsClient.layer(options).pipe(Layer.provide(NodeConnector.layer));
const wsClientLayer = (options: NatsClient.Options = {}) =>
  NatsClient.layer(options).pipe(Layer.provide(NatsConnector.layerWebSocket));

describe("NatsClient", () => {
  it.effect("connects to a real server over TCP and drains on scope close", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      let closed = false;

      yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          assert.strictEqual(client.connection.info?.host, "127.0.0.1");
          assert.strictEqual(client.connection.getServer(), `127.0.0.1:${server.port}`);
          closed = client.connection.isClosed();
        }).pipe(Effect.provide(tcpClientLayer({ servers: server.url }))),
      );

      assert.strictEqual(closed, false);
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("connects to a real server over WebSocket", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const connected = yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          return client.connection.getServer();
        }).pipe(Effect.provide(wsClientLayer({ servers: server.wsUrl }))),
      );

      assert.strictEqual(connected, `127.0.0.1:${server.wsPort}`);
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("serializes mixed TCP and WebSocket connects", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const tcp = NatsClient.make({ servers: server.url }).pipe(Effect.provide(NodeConnector.layer));
      const websocket = NatsClient.make({ servers: server.wsUrl }).pipe(Effect.provide(NatsConnector.layerWebSocket));

      yield* Effect.scoped(
        Effect.all([tcp, websocket], { concurrency: "unbounded" }).pipe(
          Effect.map(([tcpClient, wsClient]) => {
            assert.strictEqual(tcpClient.connection.getServer(), `127.0.0.1:${server.port}`);
            assert.strictEqual(wsClient.connection.getServer(), `127.0.0.1:${server.wsPort}`);
          }),
        ),
      );
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("maps graceful close to Option.none", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const closed = yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          yield* Effect.promise(() => client.connection.drain());
          return yield* client.closed;
        }).pipe(Effect.provide(tcpClientLayer({ servers: server.url }))),
      );

      assert.deepStrictEqual(closed, Option.none());
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("fails unreachable connect with a typed NATS error", () =>
    Effect.gen(function* () {
      const exit = yield* NatsClient.make({
        servers: "nats://127.0.0.1:1",
        reconnect: false,
        timeout: "50 millis",
      }).pipe(Effect.provide(NodeConnector.layer), Effect.scoped, Effect.exit);

      assert.strictEqual(exit._tag, "Failure");
    }),
  );

  it.effect("loads servers from Config", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const provider = ConfigProvider.fromEnv({ env: { NATS_URL: server.url } });
      const connected = yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          return client.connection.getServer();
        }).pipe(
          Effect.provide(
            NatsClient.layerConfig({ servers: Config.string("NATS_URL") }).pipe(
              Layer.provide(NodeConnector.layer),
              Layer.provide(Layer.succeed(ConfigProvider.ConfigProvider, provider)),
            ),
          ),
        ),
      );

      assert.strictEqual(connected, `127.0.0.1:${server.port}`);
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("loads all option fields from Config", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const connected = yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          return client.connection.getServer();
        }).pipe(
          Effect.provide(
            NatsClient.layerConfig({
              servers: Config.succeed(server.url),
              name: Config.succeed("configured"),
              auth: Config.succeed(NatsClient.Token.make({ token: Redacted.make("unused") })),
              timeout: Config.succeed("1 second"),
              pingInterval: Config.succeed("2 seconds"),
              maxPingOut: Config.succeed(3),
              reconnect: Config.succeed(false),
              noEcho: Config.succeed(true),
              inboxPrefix: Config.succeed("_INBOX.CONFIG"),
              tls: Config.succeed({}),
              ignoreClusterUpdates: Config.succeed(true),
              transformOptions: ({ tls: _tls, ...options }) => options,
            }).pipe(Layer.provide(NodeConnector.layer)),
          ),
        ),
      );

      assert.strictEqual(connected, `127.0.0.1:${server.port}`);
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("allows transformOptions to provide omitted servers", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const connected = yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          return client.connection.getServer();
        }).pipe(
          Effect.provide(
            NatsClient.layerConfig({
              transformOptions: (options) => ({ ...options, servers: server.url }),
            }).pipe(Layer.provide(NodeConnector.layer)),
          ),
        ),
      );

      assert.strictEqual(connected, `127.0.0.1:${server.port}`);
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("applies transformOptions last", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const connected = yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          return client.connection.getServer();
        }).pipe(
          Effect.provide(
            tcpClientLayer({
              servers: "nats://127.0.0.1:1",
              transformOptions: (options) => ({ ...options, servers: server.url }),
            }),
          ),
        ),
      );

      assert.strictEqual(connected, `127.0.0.1:${server.port}`);
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it("constructs Auth variants with schema class constructors", () => {
    const token = NatsClient.Token.make({ token: Redacted.make("secret") });
    const userPass = NatsClient.UserPass.make({ user: "u", pass: Redacted.make("p") });
    const creds = NatsClient.Creds.make({ creds: Redacted.make("creds") });
    const nkey = NatsClient.NKey.make({ seed: Redacted.make("seed") });

    assert.strictEqual(Predicate.isTagged(token, "Token"), true);
    assert.strictEqual(Predicate.isTagged(userPass, "UserPass"), true);
    assert.strictEqual(Predicate.isTagged(creds, "Creds"), true);
    assert.strictEqual(Predicate.isTagged(nkey, "NKey"), true);
  });

  it("translates public options into SDK connection options", () => {
    const translated = Options.translate({
      servers: ["nats://one", "nats://two"],
      name: "client-name",
      auth: NatsClient.Token.make({ token: Redacted.make("secret") }),
      timeout: "1 second",
      pingInterval: "2 seconds",
      maxPingOut: 3,
      reconnect: { maxAttempts: 4, wait: "5 seconds", jitter: "6 seconds", waitOnFirstConnect: true },
      noEcho: true,
      inboxPrefix: "_INBOX.TEST",
      tls: {},
      ignoreClusterUpdates: true,
      transformOptions: (options) => ({ ...options, name: "transformed" }),
    });

    assert.deepStrictEqual(translated.servers, ["nats://one", "nats://two"]);
    assert.strictEqual(translated.name, "transformed");
    assert.strictEqual(translated.timeout, 1_000);
    assert.strictEqual(translated.pingInterval, 2_000);
    assert.strictEqual(translated.maxPingOut, 3);
    assert.strictEqual(translated.maxReconnectAttempts, 4);
    assert.strictEqual(translated.reconnectTimeWait, 5_000);
    assert.strictEqual(translated.reconnectJitter, 6_000);
    assert.strictEqual(translated.waitOnFirstConnect, true);
    assert.strictEqual(translated.noEcho, true);
    assert.strictEqual(translated.inboxPrefix, "_INBOX.TEST");
    assert.deepStrictEqual(translated.tls, {});
    assert.strictEqual(translated.ignoreClusterUpdates, true);
    assert.isTrue(Predicate.isFunction(translated.authenticator));
    if (Predicate.isFunction(translated.authenticator)) {
      assert.deepStrictEqual(translated.authenticator(), { auth_token: "secret" });
    }
  });

  it("translates default and minimal reconnect options", () => {
    assert.deepStrictEqual(Options.translate(), {});
    assert.deepStrictEqual(Options.translate({ servers: "nats://one", reconnect: false }), {
      servers: "nats://one",
      reconnect: false,
    });
    assert.deepStrictEqual(Options.translate({ reconnect: {} }), {});
  });

  it("translates each Auth schema class", () => {
    const userPassAuthenticator = Options.translate({
      auth: NatsClient.UserPass.make({ user: "u", pass: Redacted.make("p") }),
    }).authenticator;
    const creds = Options.translate({
      auth: NatsClient.Creds.make({ creds: Redacted.make("invalid creds") }),
    }).authenticator;
    const nkey = Options.translate({
      auth: NatsClient.NKey.make({ seed: Redacted.make("invalid seed") }),
    }).authenticator;

    assert.isTrue(Predicate.isFunction(userPassAuthenticator));
    if (Predicate.isFunction(userPassAuthenticator)) {
      assert.deepStrictEqual(userPassAuthenticator(), { user: "u", pass: "p" });
    }
    assert.isFunction(creds);
    assert.isFunction(nkey);
  });

  it("maps SDK close errors to typed NATS errors", () => {
    assert.strictEqual(Errors.mapConnectError(new TimeoutError())._tag, "TimeoutError");
    assert.strictEqual(Errors.mapConnectError(new Error("boom"))._tag, "ConnectionError");
    assert.deepStrictEqual(Errors.mapClosed(), Option.none());
    assert.strictEqual(Option.getOrThrow(Errors.mapClosed(new TimeoutError()))._tag, "TimeoutError");
    assert.strictEqual(Option.getOrThrow(Errors.mapClosed(new ClosedConnectionError()))._tag, "ClosedConnectionError");
    assert.strictEqual(
      Option.getOrThrow(Errors.mapClosed(new DrainingConnectionError()))._tag,
      "DrainingConnectionError",
    );
    assert.strictEqual(Option.getOrThrow(Errors.mapClosed(new AuthorizationError("auth")))._tag, "AuthorizationError");
    assert.strictEqual(
      Option.getOrThrow(Errors.mapClosed(new UserAuthenticationExpiredError("expired")))._tag,
      "UserAuthenticationExpiredError",
    );
    assert.strictEqual(Option.getOrThrow(Errors.mapClosed(new ProtocolError("protocol")))._tag, "ProtocolError");
    assert.strictEqual(Option.getOrThrow(Errors.mapClosed(new InvalidSubjectError("bad")))._tag, "InvalidSubjectError");
    assert.strictEqual(Option.getOrThrow(Errors.mapClosed(new NoRespondersError("missing")))._tag, "NoRespondersError");
    assert.strictEqual(Option.getOrThrow(Errors.mapClosed(new Error("unknown")))._tag, "ConnectionError");
  });

  it.effect("publishes payload, headers, and reply subjects", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          const subscription = client.connection.subscribe("phase4.publish", { max: 1 });
          yield* client.publish("phase4.publish", {
            payload: encoder.encode("hello"),
            headers: { "X-Test": "yes" },
            replyTo: "_INBOX.reply",
          });

          const message = yield* Effect.promise(() => subscription[Symbol.asyncIterator]().next());
          assert.strictEqual(message.done, false);
          assert.strictEqual(decoder.decode(message.value.data), "hello");
          assert.strictEqual(message.value.headers?.get("X-Test"), "yes");
          assert.strictEqual(message.value.reply, "_INBOX.reply");
        }).pipe(Effect.provide(tcpClientLayer({ servers: server.url }))),
      );
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("fails invalid publish subjects", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const error = yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          return yield* Effect.flip(client.publish(""));
        }).pipe(Effect.provide(tcpClientLayer({ servers: server.url }))),
      );

      assert.strictEqual(error._tag, "InvalidSubjectError");
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("fails publish after scope close with ClosedConnectionError", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const client = yield* Effect.scoped(
        NatsClient.NatsClient.pipe(Effect.provide(tcpClientLayer({ servers: server.url }))),
      );

      const error = yield* Effect.flip(client.publish("phase4.closed"));

      assert.strictEqual(error._tag, "ClosedConnectionError");
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("flushes and measures rtt as an Effect Duration", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const rtt = yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          yield* client.flush;
          return yield* client.rtt;
        }).pipe(Effect.provide(tcpClientLayer({ servers: server.url }))),
      );

      assert.isTrue(Duration.toMillis(rtt) >= 0);
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("requests and receives a typed NatsMessage response", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          client.connection.subscribe("phase4.request", {
            max: 1,
            callback: (_error, message) => {
              if (Predicate.isNotUndefined(message)) {
                message.respond(
                  message.data,
                  Predicate.isNotUndefined(message.headers) ? { headers: message.headers } : {},
                );
              }
            },
          });
          const response = yield* client.request("phase4.request", {
            payload: encoder.encode("ping"),
            headers: { "X-Request": "yes" },
          });

          assert.match(response.subject, /^_INBOX\./);
          assert.strictEqual(response.text, "ping");
          assert.deepStrictEqual(NatsHeaders.get(response.headers, "X-Request"), Option.some("yes"));
        }).pipe(Effect.provide(tcpClientLayer({ servers: server.url }))),
      );
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("responds through wrapped NatsMessage values", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          const runFork = Effect.runForkWith(yield* Effect.context<never>());
          client.connection.subscribe("phase4.wrapped", {
            max: 1,
            callback: (_error, message) => {
              if (Predicate.isNotUndefined(message)) {
                runFork(
                  NatsMessage.respond(NatsMessage.fromMsg(message), {
                    payload: encoder.encode("wrapped"),
                    headers: { "X-Wrapped": "yes" },
                  }),
                );
              }
            },
          });

          const response = yield* client.request("phase4.wrapped");

          assert.strictEqual(response.text, "wrapped");
          assert.deepStrictEqual(NatsHeaders.get(response.headers, "X-Wrapped"), Option.some("yes"));
        }).pipe(Effect.provide(tcpClientLayer({ servers: server.url }))),
      );
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("surfaces no-reply failures from wrapped NatsMessage values", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          const error = yield* Deferred.make<NatsError.NoReplySubjectError>();
          const runFork = Effect.runForkWith(yield* Effect.context<never>());
          client.connection.subscribe("phase4.noReply", {
            max: 1,
            callback: (_callbackError, message) => {
              if (Predicate.isNotUndefined(message)) {
                runFork(
                  Deferred.complete(
                    error,
                    Effect.flip(NatsMessage.respond(NatsMessage.fromMsg(message))).pipe(Effect.orDie),
                  ),
                );
              }
            },
          });

          yield* client.publish("phase4.noReply");
          const failure = yield* Deferred.await(error);

          assert.strictEqual(failure._tag, "NoReplySubjectError");
          assert.strictEqual(failure.subject, "phase4.noReply");
        }).pipe(Effect.provide(tcpClientLayer({ servers: server.url }))),
      );
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("distinguishes no responders from request timeout", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      yield* Effect.scoped(
        Effect.gen(function* () {
          const client = yield* NatsClient.NatsClient;
          const start = yield* Clock.currentTimeMillis;
          const noResponders = yield* Effect.flip(client.request("phase4.noResponders", { timeout: "2 seconds" }));
          const elapsed = (yield* Clock.currentTimeMillis) - start;
          assert.strictEqual(noResponders._tag, "NoRespondersError");
          if (Predicate.isTagged(noResponders, "NoRespondersError")) {
            assert.strictEqual(noResponders.subject, "phase4.noResponders");
          }
          assert.isTrue(elapsed < 500);

          const subscription = client.connection.subscribe("phase4.timeout", { max: 1 });
          const timeout = yield* Effect.flip(client.request("phase4.timeout", { timeout: "50 millis" }));
          subscription.unsubscribe();
          assert.strictEqual(timeout._tag, "TimeoutError");
        }).pipe(Effect.provide(tcpClientLayer({ servers: server.url }))),
      );
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );
});
