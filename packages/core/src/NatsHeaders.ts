/**
 * Immutable NATS header helpers.
 *
 * @since 0.1.0
 */
import { Array as Arr, Option, Predicate, Record as Rec, Schema, String as Str } from "effect";
import { dual } from "effect/Function";
import { headers as makeMsgHdrs } from "@nats-io/nats-core";
import type { MsgHdrs } from "@nats-io/nats-core";

export const TypeId = "~effect-nats/NatsHeaders" as const;

export interface NatsHeaders extends Iterable<readonly [string, ReadonlyArray<string>]> {
  readonly [TypeId]: typeof TypeId;
}

export type Input = Readonly<Record<string, string | ReadonlyArray<string>>> | Iterable<readonly [string, string]>;

const states = new WeakMap<NatsHeaders, MsgHdrs>();

const make = (raw: MsgHdrs): NatsHeaders => {
  const headers: NatsHeaders = {
    [TypeId]: TypeId,
    [Symbol.iterator]: function* () {
      for (const [key, values] of raw) {
        const entry: readonly [string, ReadonlyArray<string>] = [key, [...values]];
        yield entry;
      }
    },
  };
  states.set(headers, raw);
  return headers;
};
export const empty: NatsHeaders = make(makeMsgHdrs());

export const isNatsHeaders = (u: unknown): u is NatsHeaders => Predicate.hasProperty(u, TypeId);

export const NatsHeaders = Schema.declare(isNatsHeaders, { identifier: "effect-nats/NatsHeaders" });

export const fromInput: (input?: Input) => NatsHeaders = (input) => {
  if (Predicate.isUndefined(input)) {
    return empty;
  }
  const headers = makeMsgHdrs();
  if (Symbol.iterator in input) {
    for (const [key, value] of input) {
      headers.append(key, value);
    }
    return make(headers);
  }
  for (const [key, value] of Rec.toEntries(input)) {
    for (const item of Arr.ensure(value)) {
      headers.append(key, item);
    }
  }
  return make(headers);
};

export const fromMsgHdrs = (input: MsgHdrs): NatsHeaders => {
  const headers = makeMsgHdrs();
  for (const [key, values] of input) {
    for (const value of values) {
      headers.append(key, value);
    }
  }
  return make(headers);
};

export const toMsgHdrs = (input: Input): MsgHdrs => raw(fromInput(input));

/* v8 ignore next -- only NatsHeaders values created in this module have access to raw state */
const raw = (self: NatsHeaders): MsgHdrs => states.get(self) ?? makeMsgHdrs();

export const get: {
  (key: string): (self: NatsHeaders) => Option.Option<string>;
  (self: NatsHeaders, key: string): Option.Option<string>;
} = dual<
  (key: string) => (self: NatsHeaders) => Option.Option<string>,
  (self: NatsHeaders, key: string) => Option.Option<string>
>(2, (self, key) => Option.liftPredicate(Str.isNonEmpty)(raw(self).get(key)));

export const getAll: {
  (key: string): (self: NatsHeaders) => ReadonlyArray<string>;
  (self: NatsHeaders, key: string): ReadonlyArray<string>;
} = dual<
  (key: string) => (self: NatsHeaders) => ReadonlyArray<string>,
  (self: NatsHeaders, key: string) => ReadonlyArray<string>
>(2, (self, key) => raw(self).values(key));

export const keys = (self: NatsHeaders): ReadonlyArray<string> => raw(self).keys();

export const toRecord = (self: NatsHeaders): Record<string, ReadonlyArray<string>> => {
  const headers = raw(self);
  return Rec.fromEntries(
    Arr.map(headers.keys(), (key) => {
      const entry: readonly [string, ReadonlyArray<string>] = [key, headers.values(key)];
      return entry;
    }),
  );
};
