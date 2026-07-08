import { assert, describe, it } from "@effect/vitest";
import { Config, ConfigProvider, Effect, Layer, Option, Predicate, Redacted } from "effect";
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
import * as NatsConnector from "effect-nats/NatsConnector";
import * as NodeConnector from "effect-nats/NodeConnector";
import * as Errors from "../src/internal/errors.ts";
import * as Options from "../src/internal/options.ts";
import * as TestNatsServer from "./utils/TestNatsServer.ts";

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
});
