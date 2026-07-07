import { assert, describe, it } from "@effect/vitest";
import { Effect } from "effect";
import * as Greeting from "effect-nats/Greeting";

describe("Greeting.make", () => {
  it.effect("creates a schema-backed greeting", () =>
    Effect.gen(function* () {
      const greeting = yield* Greeting.make({ name: "Effect" });
      assert.strictEqual(greeting.message, "Hello, Effect!");
    }),
  );
});
