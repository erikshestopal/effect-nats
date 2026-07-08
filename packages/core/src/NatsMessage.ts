/**
 * Immutable NATS message helpers.
 *
 * @since 0.1.0
 */
import { Effect, Option, Predicate, Schema } from "effect";
import type { Msg, Payload } from "@nats-io/nats-core";
import * as NatsHeaders from "./NatsHeaders.ts";
import { NoReplySubjectError } from "./NatsError.ts";

/** @since 0.1.0 @category models */
export class NatsMessage extends Schema.Class<NatsMessage>("effect-nats/NatsMessage")({
  subject: Schema.String,
  payload: Schema.Uint8Array,
  replyTo: Schema.Option(Schema.String),
  headers: NatsHeaders.NatsHeaders,
}) {
  /** @since 0.1.0 */
  get text(): string {
    return decoder.decode(this.payload);
  }

  /** @since 0.1.0 */
  json<S extends Schema.Top>(schema: S): Effect.Effect<S["Type"], Schema.SchemaError, S["DecodingServices"]> {
    return Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(this.text);
  }
}

/** @since 0.1.0 @category models */
export type RespondOptions = {
  readonly payload?: Payload;
  readonly headers?: NatsHeaders.Input;
};

const decoder = new TextDecoder();
const sdkMessages = new WeakMap<NatsMessage, Msg>();

export const fromMsg = (msg: Msg): NatsMessage => {
  const message = NatsMessage.make({
    subject: msg.subject,
    payload: msg.data,
    replyTo: Option.fromNullishOr(msg.reply),
    headers: Predicate.isNotUndefined(msg.headers) ? NatsHeaders.fromMsgHdrs(msg.headers) : NatsHeaders.empty,
  });
  sdkMessages.set(message, msg);
  return message;
};

/** @since 0.1.0 @category guards */
export const isNatsMessage = Schema.is(NatsMessage);

/** @since 0.1.0 @category combinators */
export const respond = (self: NatsMessage, options: RespondOptions = {}) => {
  const msg = sdkMessages.get(self);
  if (Predicate.isUndefined(msg)) {
    return Effect.fail(NoReplySubjectError.make({ subject: self.subject }));
  }
  return Effect.sync(() =>
    msg.respond(
      options.payload,
      Predicate.isNotUndefined(options.headers) ? { headers: NatsHeaders.toMsgHdrs(options.headers) } : {},
    ),
  ).pipe(
    Effect.andThen((responded) =>
      responded ? Effect.void : Effect.fail(NoReplySubjectError.make({ subject: self.subject })),
    ),
  );
};
