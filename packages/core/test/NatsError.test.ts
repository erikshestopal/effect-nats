import { assert, describe, it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
import * as NatsError from "effect-nats/NatsError";

describe("NatsError", () => {
  it("recognizes exported NATS errors with one schema-derived guard", () => {
    const errors = [
      new NatsError.ConnectionError({ cause: "connect failed" }),
      new NatsError.TimeoutError(),
      new NatsError.ClosedConnectionError(),
      new NatsError.DrainingConnectionError(),
      new NatsError.NoRespondersError({ subject: "svc.missing" }),
      new NatsError.RequestError({ subject: "svc.echo", cause: "boom" }),
      new NatsError.AuthorizationError(),
      new NatsError.PermissionViolationError({ operation: "publish", subject: "orders", queue: Option.none() }),
      new NatsError.ProtocolError({ cause: "bad protocol" }),
      new NatsError.InvalidSubjectError({ subject: "bad subject" }),
      new NatsError.UserAuthenticationExpiredError(),
      new NatsError.NoReplySubjectError({ subject: "events" }),
    ];

    for (const error of errors) {
      assert.isTrue(NatsError.isNatsError(error));
    }
    assert.isFalse(NatsError.isNatsError({ message: "timeout" }));
    assert.isFalse(NatsError.isNatsError(new Error("timeout")));
  });

  it.effect("decodes exported errors through the exported schema", () =>
    Effect.gen(function* () {
      const error = new NatsError.NoRespondersError({ subject: "svc.missing" });
      const decoded = yield* Schema.decodeUnknownEffect(NatsError.NoRespondersError)(error);

      assert.isTrue(NatsError.isNatsError(decoded));
      assert.strictEqual(decoded.subject, "svc.missing");
    }),
  );
});
