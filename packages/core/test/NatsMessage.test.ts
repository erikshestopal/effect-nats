import { assert, describe, it } from "@effect/vitest";
import { Effect, Option, Schema } from "effect";
import * as NatsHeaders from "effect-nats/NatsHeaders";
import * as NatsMessage from "effect-nats/NatsMessage";

const encoder = new TextEncoder();

describe("NatsMessage", () => {
  it("recognizes public message values", () => {
    const message = NatsMessage.NatsMessage.make({
      subject: "orders.created",
      payload: encoder.encode("hello"),
      replyTo: Option.some("_INBOX.1"),
      headers: NatsHeaders.empty,
    });

    assert.isTrue(NatsMessage.isNatsMessage(message));
    assert.isFalse(NatsMessage.isNatsMessage({ subject: "orders.created" }));
    assert.strictEqual(message.text, "hello");
  });

  it.effect("decodes JSON payloads with the supplied schema", () =>
    Effect.gen(function* () {
      const Order = Schema.Struct({ id: Schema.String, amountCents: Schema.Int });
      const message = NatsMessage.NatsMessage.make({
        subject: "orders.created",
        payload: encoder.encode('{"id":"o1","amountCents":1200}'),
        replyTo: Option.none(),
        headers: NatsHeaders.empty,
      });

      const order = yield* message.json(Order);
      assert.deepStrictEqual(order, { id: "o1", amountCents: 1200 });
    }),
  );

  it.effect("fails malformed JSON payloads with a SchemaError", () =>
    Effect.gen(function* () {
      const message = NatsMessage.NatsMessage.make({
        subject: "orders.created",
        payload: encoder.encode("not-json"),
        replyTo: Option.none(),
        headers: NatsHeaders.empty,
      });

      const result = yield* Effect.flip(message.json(Schema.Struct({ id: Schema.String })));
      assert.isTrue(Schema.isSchemaError(result));
    }),
  );

  it.effect("decodes through the exported schema-backed class", () =>
    Effect.gen(function* () {
      const message = NatsMessage.NatsMessage.make({
        subject: "orders.created",
        payload: encoder.encode("hello"),
        replyTo: Option.none(),
        headers: NatsHeaders.empty,
      });

      const decoded = yield* Schema.decodeUnknownEffect(NatsMessage.NatsMessage)(message);
      assert.isTrue(NatsMessage.isNatsMessage(decoded));
      assert.strictEqual(decoded.subject, "orders.created");
    }),
  );
});
