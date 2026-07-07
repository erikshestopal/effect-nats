import { assert, describe, it } from "@effect/vitest";
import { Effect } from "effect";
import * as Welcome from "@effect-nats/feature/Welcome";

describe("Welcome.make", () => {
  it.effect("composes the core workspace package", () =>
    Effect.gen(function* () {
      const message = yield* Welcome.make({ name: "Starter" });
      assert.strictEqual(message, "Hello, Starter! Welcome to Effect.");
    }),
  );
});
