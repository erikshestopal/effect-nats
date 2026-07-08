/**
 * Scenario: connection health (nats.js nats-events / rtt / stats).
 *
 * Run: `bun examples/connection-status.ts`
 */
import { Console, Duration, Effect, Stream } from "effect";
import * as NatsClient from "effect-nats/NatsClient";
import { NatsLive, runMain } from "./_shared.ts";

const program = Effect.gen(function* () {
  const nats = yield* NatsClient.NatsClient;

  const rtt = yield* nats.rtt;
  const stats = yield* nats.stats;
  yield* nats.flush;

  // Quiet connections often emit nothing; timeout completes the stream empty.
  const statuses = yield* nats.status.pipe(Stream.take(3), Stream.timeout("1 second"), Stream.runCollect);

  const result = {
    server: nats.connection.getServer(),
    rttMillis: Duration.toMillis(rtt),
    inMsgs: stats.inMsgs,
    outMsgs: stats.outMsgs,
    statusEvents: [...statuses].map((status) => status.type),
  };

  yield* Console.log(result);
  return result;
}).pipe(Effect.scoped, Effect.provide(NatsLive));

runMain({ label: "connection-status", effect: program });
