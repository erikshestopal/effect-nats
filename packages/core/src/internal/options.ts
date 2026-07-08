import { Array as Arr, Duration, Match, Predicate, Redacted } from "effect";
import {
  type Authenticator,
  credsAuthenticator,
  nkeyAuthenticator,
  tokenAuthenticator,
  usernamePasswordAuthenticator,
} from "@nats-io/nats-core";
import type { Auth, Options } from "../NatsClient.ts";
import type { ConnectionOptions } from "@nats-io/nats-core";

const encoder = new TextEncoder();

const millis = (duration: Duration.Input): number => Duration.toMillis(duration);

const authToSdk = (auth: Auth): Authenticator =>
  Match.value(auth).pipe(
    Match.tag("UserPass", ({ user, pass }) => usernamePasswordAuthenticator(user, Redacted.value(pass))),
    Match.tag("Token", ({ token }) => tokenAuthenticator(Redacted.value(token))),
    Match.tag("Creds", ({ creds }) => credsAuthenticator(encoder.encode(Redacted.value(creds)))),
    Match.tag("NKey", ({ seed }) => nkeyAuthenticator(encoder.encode(Redacted.value(seed)))),
    Match.exhaustive,
  );

export const translate = (options: Options = {}): ConnectionOptions => {
  const reconnect = options.reconnect;
  const servers = Predicate.isNotUndefined(options.servers)
    ? Predicate.isString(options.servers)
      ? options.servers
      : Arr.fromIterable(options.servers)
    : undefined;
  const authenticator = Predicate.isNotUndefined(options.auth) ? authToSdk(options.auth) : undefined;
  const translated: ConnectionOptions = {
    ...(Predicate.isNotUndefined(servers) ? { servers } : {}),
    ...(Predicate.isNotUndefined(options.name) ? { name: options.name } : {}),
    ...(Predicate.isNotUndefined(authenticator) ? { authenticator } : {}),
    ...(Predicate.isNotUndefined(options.timeout) ? { timeout: millis(options.timeout) } : {}),
    ...(Predicate.isNotUndefined(options.pingInterval) ? { pingInterval: millis(options.pingInterval) } : {}),
    ...(Predicate.isNotUndefined(options.maxPingOut) ? { maxPingOut: options.maxPingOut } : {}),
    ...(Predicate.isNotUndefined(options.noEcho) ? { noEcho: options.noEcho } : {}),
    ...(Predicate.isNotUndefined(options.inboxPrefix) ? { inboxPrefix: options.inboxPrefix } : {}),
    ...(Predicate.isNotUndefined(options.tls) ? { tls: options.tls } : {}),
    ...(Predicate.isNotUndefined(options.ignoreClusterUpdates)
      ? { ignoreClusterUpdates: options.ignoreClusterUpdates }
      : {}),
    ...(Predicate.isNotUndefined(reconnect)
      ? Predicate.isBoolean(reconnect)
        ? { reconnect }
        : {
            ...(Predicate.isNotUndefined(reconnect.maxAttempts) ? { maxReconnectAttempts: reconnect.maxAttempts } : {}),
            ...(Predicate.isNotUndefined(reconnect.wait) ? { reconnectTimeWait: millis(reconnect.wait) } : {}),
            ...(Predicate.isNotUndefined(reconnect.jitter) ? { reconnectJitter: millis(reconnect.jitter) } : {}),
            ...(Predicate.isNotUndefined(reconnect.waitOnFirstConnect)
              ? { waitOnFirstConnect: reconnect.waitOnFirstConnect }
              : {}),
          }
      : {}),
  };
  return Predicate.isNotUndefined(options.transformOptions) ? options.transformOptions(translated) : translated;
};
