import { assert, describe, it } from "@effect/vitest";
import { Option } from "effect";
import * as NatsHeaders from "effect-nats/NatsHeaders";

describe("NatsHeaders", () => {
  it("builds immutable header views from records", () => {
    const headers = NatsHeaders.fromInput({
      one: "a",
      many: ["b", "c"],
      "X-Nats-Case": "preserved",
    });

    assert.isTrue(NatsHeaders.isNatsHeaders(headers));
    assert.deepStrictEqual(NatsHeaders.get(headers, "one"), Option.some("a"));
    assert.deepStrictEqual(NatsHeaders.get(headers, "missing"), Option.none());
    assert.deepStrictEqual(NatsHeaders.getAll(headers, "many"), ["b", "c"]);
    assert.deepStrictEqual(NatsHeaders.keys(headers), ["one", "many", "X-Nats-Case"]);
    assert.deepStrictEqual(NatsHeaders.toRecord(headers), {
      one: ["a"],
      many: ["b", "c"],
      "X-Nats-Case": ["preserved"],
    });
  });

  it("builds immutable header views from iterables", () => {
    const headers = NatsHeaders.fromInput([
      ["x", "1"],
      ["x", "2"],
    ]);

    assert.deepStrictEqual(NatsHeaders.getAll(headers, "x"), ["1", "2"]);
    assert.deepStrictEqual(Array.from(headers), [["x", ["1", "2"]]]);
  });

  it("empty iterates to nothing", () => {
    assert.deepStrictEqual(Array.from(NatsHeaders.empty), []);
  });

  it("defaults absent input to empty headers", () => {
    assert.deepStrictEqual(Array.from(NatsHeaders.fromInput()), []);
  });
});
