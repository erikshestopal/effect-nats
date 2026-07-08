import { Context, Effect, Layer } from "effect";
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { spawn } from "node:child_process";
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off
import { join } from "node:path";
// @effect-diagnostics-next-line nodeBuiltinImport:off
import type { ChildProcess } from "node:child_process";

export class TestNatsServer extends Context.Service<
  TestNatsServer,
  {
    readonly port: number;
    readonly url: string;
    readonly wsPort: number;
    readonly wsUrl: string;
  }
>()("test/TestNatsServer") {}

type PortsFile = {
  readonly nats: ReadonlyArray<string>;
  readonly websocket: ReadonlyArray<string>;
};

type Running = {
  readonly child: ChildProcess;
  readonly tmp: string;
};

const start = (jetStream: boolean) =>
  Effect.acquireRelease(
    Effect.gen(function* () {
      const tmp = yield* Effect.promise(() => mkdtemp(join(tmpdir(), "effect-nats-")));
      const store = join(tmp, "store");
      const config = join(tmp, "nats.conf");
      yield* Effect.promise(() =>
        writeFile(
          config,
          [
            "host: 127.0.0.1",
            "port: -1",
            `ports_file_dir: "${tmp}"`,
            "websocket {",
            "  host: 127.0.0.1",
            "  port: -1",
            "  no_tls: true",
            "}",
          ].join("\n"),
        ),
      );
      const args = ["-c", config, ...(jetStream ? ["-js", "-sd", store] : [])];
      const child = spawn("nats-server", args, { stdio: ["ignore", "ignore", "pipe"] });
      const running = { child, tmp };
      const pid = child.pid ?? 0;
      const ports = yield* waitForPorts({ running, pid }).pipe(Effect.timeout("5 seconds"));
      const natsUrl = ports.nats[0] ?? "";
      const wsUrl = ports.websocket[0] ?? "";
      return {
        running,
        server: TestNatsServer.of({
          port: Number(new URL(natsUrl).port),
          url: natsUrl,
          wsPort: Number(new URL(wsUrl).port),
          wsUrl,
        }),
      };
    }),
    ({ running }) => stop(running),
  ).pipe(Effect.map(({ server }) => server));

const waitForPorts = (options: { readonly running: Running; readonly pid: number }) =>
  Effect.callback<PortsFile, string>((resume) => {
    const portsFile = join(options.running.tmp, `nats-server_${options.pid}.ports`);
    const check = () => {
      readFile(portsFile, "utf8").then(
        (content) => {
          clearInterval(interval);
          resume(Effect.succeed(JSON.parse(content)));
        },
        () => {
          if (options.running.child.exitCode !== null) {
            clearInterval(interval);
            resume(Effect.fail("nats-server exited before writing ports file"));
          }
        },
      );
    };
    // @effect-diagnostics-next-line globalTimersInEffect:off
    const interval = setInterval(check, 25);
    check();
    return Effect.sync(() => clearInterval(interval));
  });

const stop = (running: Running) =>
  Effect.callback<void>((resume) => {
    const onExit = () => {
      resume(Effect.promise(() => rm(running.tmp, { force: true, recursive: true })));
    };
    running.child.once("exit", onExit);
    running.child.kill("SIGTERM");
    return Effect.sync(() => running.child.off("exit", onExit));
  });

export const layer: Layer.Layer<TestNatsServer> = Layer.effect(TestNatsServer, start(false).pipe(Effect.orDie));

export const layerJetStream: Layer.Layer<TestNatsServer> = Layer.effect(TestNatsServer, start(true).pipe(Effect.orDie));
