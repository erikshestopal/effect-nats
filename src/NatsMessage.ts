/**
 * Immutable NATS message helpers.
 *
 * @since 0.1.0
 */
import { Effect, Option, Predicate, Schema } from "effect";
import type { Msg, Payload } from "@nats-io/nats-core";
import * as NatsHeaders from "./NatsHeaders.ts";
import { NoReplySubjectError } from "./NatsError.ts";

export class NatsMessage extends Schema.Class<NatsMessage>("effect-nats/NatsMessage")({
  subject: Schema.String,
  payload: Schema.Uint8Array,
  replyTo: Schema.Option(Schema.String),
  headers: NatsHeaders.NatsHeaders,
}) {
  get text(): string {
    return decoder.decode(this.payload);
  }

  json<S extends Schema.Top>(schema: S): Effect.Effect<S["Type"], Schema.SchemaError, S["DecodingServices"]> {
    return Schema.decodeUnknownEffect(Schema.fromJsonString(schema))(this.text);
  }
}

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
    headers: Option.getOrElse(
      Option.map(Option.fromUndefinedOr(msg.headers), NatsHeaders.fromMsgHdrs),
      () => NatsHeaders.empty,
    ),
  });
  sdkMessages.set(message, msg);
  return message;
};

export const isNatsMessage = Schema.is(NatsMessage);

export const respond = (self: NatsMessage, options: RespondOptions = {}) =>
  Effect.fromOption(Option.fromNullishOr(sdkMessages.get(self)), () =>
    NoReplySubjectError.make({ subject: self.subject }),
  ).pipe(
    Effect.flatMap((msg) =>
      Effect.sync(() =>
        msg.respond(
          options.payload,
          Predicate.isNotUndefined(options.headers) ? { headers: NatsHeaders.toMsgHdrs(options.headers) } : {},
        ),
      ),
    ),
    Effect.flatMap((responded) =>
      responded ? Effect.void : Effect.fail(NoReplySubjectError.make({ subject: self.subject })),
    ),
  );
