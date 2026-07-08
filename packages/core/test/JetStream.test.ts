import { assert, describe, it } from "@effect/vitest";
import {
  Array as Arr,
  DateTime,
  Effect,
  Equal,
  Layer,
  Number as Num,
  Option,
  Predicate,
  Ref,
  Schema,
  Stream,
} from "effect";
import { DeliverPolicy } from "@nats-io/jetstream";
import * as JetStream from "effect-nats/JetStream";
import * as JsMessage from "effect-nats/JsMessage";
import * as NatsClient from "effect-nats/NatsClient";
import * as NatsHeaders from "effect-nats/NatsHeaders";
import * as NodeConnector from "effect-nats/NodeConnector";
import * as TestNatsServer from "./utils/TestNatsServer.ts";
import { withConsumer, withStream } from "./utils/jsFixtures.ts";

const encoder = new TextEncoder();
const JsonPayload = Schema.Struct({ id: Schema.String });

const clientLayer = (options: NatsClient.Options = {}) =>
  NatsClient.layer(options).pipe(Layer.provide(NodeConnector.layer));

const jetStreamLayer = (options: NatsClient.Options = {}) =>
  JetStream.layer().pipe(Layer.provide(clientLayer(options)));

describe("JetStream", () => {
  it.effect("publishes acknowledged messages with sequence numbers", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const ack = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE6_PUB", subjects: ["phase6.pub"] });
          const js = yield* JetStream.JetStream;
          return yield* js.publish("phase6.pub", { payload: encoder.encode("one") });
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.isTrue(
        Equal.equals(
          ack,
          JetStream.PubAck.make({ stream: "PHASE6_PUB", seq: 1, duplicate: false, domain: Option.none() }),
        ),
      );
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("deduplicates repeated msgID publishes", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const duplicate = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE6_DEDUP", subjects: ["phase6.dedup"] });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase6.dedup", { msgID: "same" });
          return yield* js.publish("phase6.dedup", { msgID: "same" });
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.strictEqual(duplicate.duplicate, true);
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("maps wrong last subject sequence expectations", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE6_CAS", subjects: ["phase6.cas"] });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase6.cas");
          const error = yield* Effect.flip(js.publish("phase6.cas", { expect: { lastSubjectSequence: 0 } }));
          const ack = yield* js.publish("phase6.cas", { expect: { lastSubjectSequence: 1 } });
          return { error, ack };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.strictEqual(result.error._tag, "WrongLastSequenceError");
      assert.strictEqual(result.ack.seq, 2);
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("maps stream expectation mismatches to JetStreamApiError", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const error = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE6_EXPECT", subjects: ["phase6.expect"] });
          const js = yield* JetStream.JetStream;
          return yield* Effect.flip(js.publish("phase6.expect", { expect: { streamName: "OTHER" } }));
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.strictEqual(error._tag, "JetStreamApiError");
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("maps missing JetStream support", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const error = yield* Effect.scoped(
        Effect.gen(function* () {
          const js = yield* JetStream.JetStream;
          return yield* Effect.flip(js.publish("phase6.disabled"));
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.strictEqual(error._tag, "JetStreamNotEnabledError");
    }).pipe(Effect.provide(TestNatsServer.layer)),
  );

  it.effect("resolves durable and ordered consumers and maps missing resources", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE7_CONSUMER", subjects: ["phase7.consumer"] });
          yield* withConsumer({ stream: "PHASE7_CONSUMER", name: "durable" });
          const js = yield* JetStream.JetStream;
          const durable = yield* js.consumer("PHASE7_CONSUMER", "durable");
          const ordered = yield* js.consumer("PHASE7_CONSUMER", { deliver_policy: DeliverPolicy.All });
          const durableInfo = yield* durable.info();
          const orderedInfo = yield* ordered.info();
          const missingStream = yield* Effect.flip(js.consumer("PHASE7_MISSING", "durable"));
          const missingConsumer = yield* Effect.flip(js.consumer("PHASE7_CONSUMER", "missing"));
          return { durableInfo, orderedInfo, missingStream, missingConsumer };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.strictEqual(result.durableInfo.name, "durable");
      assert.isObject(result.orderedInfo);
      assert.isTrue(Predicate.isTagged(result.missingStream, "StreamNotFoundError"));
      assert.isTrue(Predicate.isTagged(result.missingConsumer, "ConsumerNotFoundError"));
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("next returns JsMessage data and none when the pull expires", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE7_NEXT", subjects: ["phase7.next"] });
          yield* withConsumer({ stream: "PHASE7_NEXT", name: "durable" });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase7.next", {
            payload: encoder.encode('{"id":"one"}'),
            headers: { "Nats-Test": "present" },
          });
          const consumer = yield* js.consumer("PHASE7_NEXT", "durable");
          const message = yield* consumer.next({ expires: "1 second" }).pipe(Effect.map(Option.getOrThrow));
          const decoded = yield* message.json(JsonPayload);
          const messages = yield* JsMessage.JsMessageService;
          yield* messages.ack(message);
          const empty = yield* consumer.next({ expires: "1 second" });
          return { message, decoded, empty };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.isTrue(JsMessage.isJsMessage(result.message));
      assert.strictEqual(result.message.subject, "phase7.next");
      assert.strictEqual(result.message.text, '{"id":"one"}');
      assert.strictEqual(Option.getOrThrow(NatsHeaders.get(result.message.headers, "Nats-Test")), "present");
      assert.deepStrictEqual(result.decoded, { id: "one" });
      assert.strictEqual(result.message.stream, "PHASE7_NEXT");
      assert.strictEqual(result.message.consumer, "durable");
      assert.strictEqual(result.message.seq, 1);
      assert.strictEqual(result.message.deliveryCount, 1);
      assert.strictEqual(result.message.redelivered, false);
      assert.isTrue(DateTime.isUtc(result.message.time));
      assert.isTrue(Option.isNone(result.empty));
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("confirmAck confirms the first ack and suppresses redelivery", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const second = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE7_ACK", subjects: ["phase7.ack"] });
          yield* withConsumer({ stream: "PHASE7_ACK", name: "durable", ackWaitNanos: 200_000_000 });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase7.ack");
          const consumer = yield* js.consumer("PHASE7_ACK", "durable");
          const message = yield* consumer.next().pipe(Effect.map(Option.getOrThrow));
          assert.strictEqual(yield* JsMessage.confirmAck(message), true);
          return yield* consumer.next({ expires: "1 second" });
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.isTrue(Option.isNone(second));
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("nak with delay redelivers after the delay", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const redelivered = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE7_NAK", subjects: ["phase7.nak"] });
          yield* withConsumer({ stream: "PHASE7_NAK", name: "durable", ackWaitNanos: 5_000_000_000 });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase7.nak");
          const consumer = yield* js.consumer("PHASE7_NAK", "durable");
          const first = yield* consumer.next().pipe(Effect.map(Option.getOrThrow));
          const messages = yield* JsMessage.JsMessageService;
          yield* messages.nak(first, { delay: "100 millis" });
          return yield* consumer.next({ expires: "1 second" }).pipe(Effect.map(Option.getOrThrow));
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.strictEqual(redelivered.redelivered, true);
      assert.strictEqual(redelivered.deliveryCount, 2);
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("term prevents redelivery", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const second = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE7_TERM", subjects: ["phase7.term"] });
          yield* withConsumer({ stream: "PHASE7_TERM", name: "durable", ackWaitNanos: 200_000_000 });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase7.term");
          const consumer = yield* js.consumer("PHASE7_TERM", "durable");
          const first = yield* consumer.next().pipe(Effect.map(Option.getOrThrow));
          const messages = yield* JsMessage.JsMessageService;
          yield* messages.term(first, { reason: "done" });
          return yield* consumer.next({ expires: "1 second" });
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.isTrue(Option.isNone(second));
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.live(
    "working extends the ack wait",
    () =>
      Effect.gen(function* () {
        const server = yield* TestNatsServer.TestNatsServer;
        const second = yield* Effect.scoped(
          Effect.gen(function* () {
            yield* withStream({ name: "PHASE7_WORKING", subjects: ["phase7.working"] });
            yield* withConsumer({ stream: "PHASE7_WORKING", name: "durable", ackWaitNanos: 200_000_000 });
            const js = yield* JetStream.JetStream;
            yield* js.publish("phase7.working");
            const consumer = yield* js.consumer("PHASE7_WORKING", "durable");
            const first = yield* consumer.next().pipe(Effect.map(Option.getOrThrow));
            yield* Effect.sleep("100 millis");
            yield* JsMessage.working(first);
            yield* Effect.sleep("150 millis");
            yield* JsMessage.ack(first);
            return yield* consumer.next({ expires: "1 second" });
          }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
        );

        assert.isTrue(Option.isNone(second));
      }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
    { timeout: 10_000 },
  );

  it.effect("tapAck acks success and naks typed failures", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE7_PROCESS", subjects: ["phase7.process"] });
          yield* withConsumer({ stream: "PHASE7_PROCESS", name: "durable", ackWaitNanos: 5_000_000_000 });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase7.process", { payload: encoder.encode("success") });
          yield* js.publish("phase7.process", { payload: encoder.encode("failure") });
          yield* js.publish("phase7.process", { payload: encoder.encode("failure-now") });
          const consumer = yield* js.consumer("PHASE7_PROCESS", "durable");
          const success = yield* consumer.next().pipe(Effect.map(Option.getOrThrow));
          yield* Stream.make(success).pipe(
            JsMessage.tapAck(() => Effect.void),
            Stream.runDrain,
          );
          const failure = yield* consumer.next().pipe(Effect.map(Option.getOrThrow));
          yield* Stream.make(failure).pipe(
            JsMessage.tapAck(() => Effect.fail("bad"), { nakDelay: "100 millis" }),
            Stream.runDrain,
          );
          const immediate = yield* consumer.next().pipe(Effect.map(Option.getOrThrow));
          yield* Stream.make(immediate).pipe(
            JsMessage.tapAck(() => Effect.fail("bad")),
            Stream.runDrain,
          );
          const redelivery1 = yield* consumer.next({ expires: "1 second" }).pipe(Effect.map(Option.getOrThrow));
          const redelivery2 = yield* consumer.next({ expires: "1 second" }).pipe(Effect.map(Option.getOrThrow));
          return [redelivery1, redelivery2];
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.sameMembers(
        result.map((message) => message.text),
        ["failure", "failure-now"],
      );
      assert.deepStrictEqual(
        result.map((message) => message.deliveryCount),
        [2, 2],
      );
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("mapEffectAcked emits handler values and acknowledges processed messages", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE7_MAP_ACKED", subjects: ["phase7.mapAcked"] });
          yield* withConsumer({ stream: "PHASE7_MAP_ACKED", name: "durable" });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase7.mapAcked", { payload: encoder.encode("one") });
          yield* js.publish("phase7.mapAcked", { payload: encoder.encode("two") });
          const consumer = yield* js.consumer("PHASE7_MAP_ACKED", "durable");
          const texts = yield* consumer.fetch({ maxMessages: 2, expires: "1 second" }).pipe(
            JsMessage.mapEffectAcked((message) => Effect.succeed(message.text)),
            Stream.runCollect,
          );
          const after = yield* consumer.next({ expires: "1 second" });
          return { texts, after };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.deepStrictEqual([...result.texts], ["one", "two"]);
      assert.isTrue(Option.isNone(result.after));
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("confirmAck returns false for schema-made messages without an SDK reply", () =>
    Effect.gen(function* () {
      const time = yield* DateTime.now;
      const confirmed = yield* JsMessage.confirmAck(
        JsMessage.JsMessage.make({
          subject: "phase7.synthetic",
          payload: encoder.encode("synthetic"),
          replyTo: Option.none(),
          headers: NatsHeaders.empty,
          stream: "PHASE7_SYNTHETIC",
          consumer: "durable",
          seq: 1,
          deliveryCount: 1,
          redelivered: false,
          pending: 0,
          time,
        }),
      );
      assert.strictEqual(confirmed, false);
    }).pipe(Effect.provide(JsMessage.layer)),
  );

  it.effect("tapAck terms defects without failing", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const second = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE7_DEFECT", subjects: ["phase7.defect"] });
          yield* withConsumer({ stream: "PHASE7_DEFECT", name: "durable", ackWaitNanos: 200_000_000 });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase7.defect");
          const consumer = yield* js.consumer("PHASE7_DEFECT", "durable");
          const message = yield* consumer.next().pipe(Effect.map(Option.getOrThrow));
          yield* Stream.make(message).pipe(
            JsMessage.tapAck(() => Effect.die("boom")),
            Stream.runDrain,
          );
          return yield* consumer.next({ expires: "1 second" });
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.isTrue(Option.isNone(second));
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("fetch yields bounded streams and completes on expiry", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE7_FETCH", subjects: ["phase7.fetch"] });
          yield* withConsumer({ stream: "PHASE7_FETCH", name: "durable" });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase7.fetch", { payload: encoder.encode("one") });
          yield* js.publish("phase7.fetch", { payload: encoder.encode("two") });
          const consumer = yield* js.consumer("PHASE7_FETCH", "durable");
          const first = yield* consumer.fetch({ maxMessages: 2, expires: "1 second" }).pipe(Stream.runCollect);
          const empty = yield* consumer.fetch({ maxMessages: 2, expires: "1 second" }).pipe(Stream.runCollect);
          return { first, empty };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.deepStrictEqual(
        [...result.first].map((message) => message.text),
        ["one", "two"],
      );
      assert.strictEqual(result.empty.length, 0);
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("consume continuously refills pulls and lets tapAck ack messages", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const result = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE8_CONSUME", subjects: ["phase8.consume"] });
          yield* withConsumer({ stream: "PHASE8_CONSUME", name: "durable" });
          const js = yield* JetStream.JetStream;
          for (let index = 0; index < 50; index++) {
            yield* js.publish("phase8.consume", { payload: encoder.encode(String(index)) });
          }
          const notifications = yield* Ref.make(0);
          const consumer = yield* js.consumer("PHASE8_CONSUME", "durable");
          const consumed = yield* consumer
            .consume({
              maxMessages: 10,
              onNotification: () => Ref.update(notifications, Num.increment),
            })
            .pipe(
              JsMessage.tapAck(() => Effect.void),
              Stream.take(50),
              Stream.runCollect,
            );
          const after = yield* consumer.next({ expires: "1 second" });
          const notificationCount = yield* Ref.get(notifications);
          return { consumed, after, notificationCount };
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.deepStrictEqual(
        [...result.consumed].map((message) => message.text),
        Arr.makeBy(50, String),
      );
      assert.isTrue(Option.isNone(result.after));
      assert.isAbove(result.notificationCount, 0);
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("consume ignores notification handler failures", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const texts = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE8_NOTIFY", subjects: ["phase8.notify"] });
          yield* withConsumer({ stream: "PHASE8_NOTIFY", name: "durable" });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase8.notify", { payload: encoder.encode("one") });
          yield* js.publish("phase8.notify", { payload: encoder.encode("two") });
          const consumer = yield* js.consumer("PHASE8_NOTIFY", "durable");
          return yield* consumer
            .consume({
              maxMessages: 1,
              onNotification: () => Effect.die("ignored"),
            })
            .pipe(
              JsMessage.tapAck(() => Effect.void),
              Stream.take(2),
              Stream.map((message) => message.text),
              Stream.runCollect,
            );
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.deepStrictEqual([...texts], ["one", "two"]);
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );

  it.effect("consume works without a notification handler", () =>
    Effect.gen(function* () {
      const server = yield* TestNatsServer.TestNatsServer;
      const text = yield* Effect.scoped(
        Effect.gen(function* () {
          yield* withStream({ name: "PHASE8_PLAIN", subjects: ["phase8.plain"] });
          yield* withConsumer({ stream: "PHASE8_PLAIN", name: "durable" });
          const js = yield* JetStream.JetStream;
          yield* js.publish("phase8.plain", { payload: encoder.encode("plain") });
          const consumer = yield* js.consumer("PHASE8_PLAIN", "durable");
          return yield* consumer.consume({ maxMessages: 1 }).pipe(
            JsMessage.tapAck(() => Effect.void),
            Stream.take(1),
            Stream.runHead,
            Effect.map(Option.getOrThrow),
            Effect.map((message) => message.text),
          );
        }).pipe(Effect.provide(jetStreamLayer({ servers: server.url }))),
      );

      assert.strictEqual(text, "plain");
    }).pipe(Effect.provide(TestNatsServer.layerJetStream)),
  );
});
