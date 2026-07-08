/**
 * Scenario: connection health (nats.js nats-events / rtt / stats).
 *
 * Run: `bun examples/connection-status.ts`
 */
import { Console, Duration, Effect } from "effect";
import * as NatsClient from "effect-nats/NatsClient";
import { NatsLive, runMain } from "./_shared.ts";

const program = Effect.gen(function* () {
  const nats = yield* NatsClient.NatsClient;

  const rtt = yield* nats.rtt;
  const stats = yield* nats.stats;
  yield* nats.flush;

  const result = {
    server: nats.connection.getServer(),
    rttMillis: Duration.toMillis(rtt),
    inMsgs: stats.inMsgs,
    outMsgs: stats.outMsgs,
    statusEvents: "subscribe to nats.status for reconnect/disconnect events",
  };

  yield* Console.log(result);
  return result;
}).pipe(Effect.scoped, Effect.provide(NatsLive));

runMain({ label: "connection-status", effect: program });
