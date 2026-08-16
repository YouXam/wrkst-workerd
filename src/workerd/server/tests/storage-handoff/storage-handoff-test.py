import concurrent.futures
import http.client
import json
import os
import queue
import select
import signal
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

WORKERD_BINARY = sys.argv.pop(1)


WORKER_SOURCE = r"""
import { DurableObject } from "cloudflare:workers";

export default {
  async fetch(request, env) {
    const url = new URL(request.url);
    const path = url.pathname;
    if (path === "/plain") return new Response(env.GENERATION);
    if (path === "/overflow") {
      const controller = new AbortController();
      const batch = url.searchParams.get("batch") ?? "default";
      const calls = Array.from({length: 1025}, (_, index) => {
        const stub = env.COUNTER.get(env.COUNTER.idFromName(`${batch}-${index}`));
        return stub.fetch("http://actor/noop", {signal: controller.signal});
      });
      try {
        await Promise.all(calls);
        return new Response("not overloaded");
      } catch (error) {
        controller.abort();
        await Promise.allSettled(calls);
        return new Response(error.message.includes("Too many requests queued")
          ? "overloaded"
          : error.message);
      }
    }

    const actorName = path === "/schedule-alarm" ? "alarm" : "counter";
    const stub = env.COUNTER.get(env.COUNTER.idFromName(actorName));
    return stub.fetch(request);
  }
};

export class Counter extends DurableObject {
  constructor(ctx, env) {
    super(ctx, env);
    this.ctx = ctx;
    this.env = env;
  }

  async fetch(request) {
    const path = new URL(request.url).pathname;
    switch (path) {
      case "/setup":
        await this.ctx.storage.put("value", 0);
        this.ctx.waitUntil((async () => {
          await this.env.COORDINATOR.fetch("http://coordinator/start-transaction");
          let result;
          try {
            await this.ctx.storage.transaction(async (txn) => {
              const value = (await txn.get("value")) ?? 0;
              await this.env.COORDINATOR.fetch("http://coordinator/hold");
              await txn.put("value", value + 100);
            });
            result = "transaction committed";
          } catch (error) {
            result = error.message;
          }
          await this.env.COORDINATOR.fetch("http://coordinator/report", {
            method: "POST",
            body: result,
          });
        })());
        return new Response("started");

      case "/schedule-alarm":
        await this.ctx.storage.setAlarm(Date.now() + 5000);
        return new Response("scheduled");

      case "/seed":
        await this.ctx.storage.put("value", 7);
        return new Response("seeded");

      case "/increment": {
        await this.env.COORDINATOR.fetch(
          `http://coordinator/entered/${this.env.GENERATION}`);
        this.ctx.storage.sql.exec(
          "CREATE TABLE IF NOT EXISTS handoff_probe (generation TEXT NOT NULL)");
        this.ctx.storage.sql.exec(
          "INSERT INTO handoff_probe (generation) VALUES (?)", this.env.GENERATION);
        const value = (await this.ctx.storage.get("value")) ?? 0;
        await this.ctx.storage.put("value", value + 1);
        return new Response(String(value + 1));
      }

      case "/read":
        return new Response(String((await this.ctx.storage.get("value")) ?? 0));

      case "/noop":
        return new Response("noop");

      default:
        return new Response("not found", {status: 404});
    }
  }

  async alarm() {
    await this.env.COORDINATOR.fetch(
      `http://coordinator/alarm/${this.env.GENERATION}`);
  }
}
"""


class CoordinatorHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/hold":
            self.server.state.hold_entered.set()
            self.server.state.hold_release.wait(20)
        elif self.path == "/start-transaction":
            self.server.state.transaction_start_entered.set()
            self.server.state.transaction_start_release.wait(20)
        elif self.path.startswith("/entered/"):
            self.server.state.entered.put(self.path.removeprefix("/entered/"))
        elif self.path.startswith("/alarm/"):
            self.server.state.alarms.put(self.path.removeprefix("/alarm/"))
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", "0"))
        self.server.state.reports.put(self.rfile.read(length).decode())
        self.send_response(200)
        self.send_header("Content-Length", "0")
        self.end_headers()

    def log_message(self, format, *args):
        pass


class Coordinator:
    def __init__(self):
        self.hold_entered = threading.Event()
        self.hold_release = threading.Event()
        self.transaction_start_entered = threading.Event()
        self.transaction_start_release = threading.Event()
        self.entered = queue.Queue()
        self.alarms = queue.Queue()
        self.reports = queue.Queue()
        self.server = ThreadingHTTPServer(("127.0.0.1", 0), CoordinatorHandler)
        self.server.state = self
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def address(self):
        host, port = self.server.server_address
        return f"{host}:{port}"

    def close(self):
        self.hold_release.set()
        self.transaction_start_release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class WorkerdProcess:
    def __init__(self, binary, directory, storage, coordinator, generation):
        self.binary = binary
        self.directory = directory
        self.storage = storage
        self.coordinator = coordinator
        self.generation = generation
        self.process = None
        self.port = None
        self.stderr = directory / "stderr.log"
        self._write_files()

    def _write_files(self):
        (self.directory / "worker.js").write_text(WORKER_SOURCE)
        config = f"""
using Workerd = import "/workerd/workerd.capnp";

const config :Workerd.Config = (
  services = [
    (name = "coordinator", external = (
      address = {json.dumps(self.coordinator)},
      http = (),
    )),
    (name = "main", worker = (
      compatibilityDate = "2026-04-01",
      modules = [(name = "worker.js", esModule = embed "worker.js")],
      bindings = [
        (name = "COORDINATOR", service = "coordinator"),
        (name = "GENERATION", text = {json.dumps(self.generation)}),
        (name = "COUNTER", durableObjectNamespace = "Counter"),
      ],
      durableObjectNamespaces = [(
        className = "Counter",
        uniqueKey = "handoff-test",
        enableSql = true,
      )],
      durableObjectStorage = (localDisk = "disk"),
    )),
    (name = "disk", disk = (
      path = {json.dumps(str(self.storage))},
      writable = true,
    )),
  ],
  sockets = [(name = "http", address = "127.0.0.1:0", http = (), service = "main")],
);
"""
        (self.directory / "config.capnp").write_text(config)

    def start(self):
        control_read, control_write = os.pipe()
        stderr = self.stderr.open("w")
        self.process = subprocess.Popen(
            [
                self.binary,
                "serve",
                str(self.directory / "config.capnp"),
                f"--control-fd={control_write}",
            ],
            pass_fds=(control_write,),
            stdout=subprocess.DEVNULL,
            stderr=stderr,
            text=True,
        )
        stderr.close()
        os.close(control_write)
        try:
            deadline = time.monotonic() + 15
            pending = b""
            while time.monotonic() < deadline:
                readable, _, _ = select.select([control_read], [], [], 0.2)
                if not readable:
                    if self.process.poll() is not None:
                        raise AssertionError(self.read_stderr())
                    continue
                chunk = os.read(control_read, 4096)
                if not chunk:
                    raise AssertionError(self.read_stderr())
                pending += chunk
                while b"\n" in pending:
                    line, pending = pending.split(b"\n", 1)
                    message = json.loads(line)
                    if (
                        message.get("event") == "listen"
                        and message.get("socket") == "http"
                    ):
                        self.port = message["port"]
                        return
            raise TimeoutError(
                f"{self.generation} workerd did not listen: {self.read_stderr()}"
            )
        finally:
            os.close(control_read)

    def send_signal(self, sig):
        self.process.send_signal(sig)

    def wait(self, timeout=10):
        return self.process.wait(timeout=timeout)

    def read_stderr(self):
        return self.stderr.read_text() if self.stderr.exists() else ""

    def cleanup(self):
        if self.process is None or self.process.poll() is not None:
            return
        self.process.kill()
        self.process.wait(timeout=5)


def request(port, path, timeout=10):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=timeout)
    try:
        connection.request("GET", path)
        response = connection.getresponse()
        body = response.read().decode()
        if response.status != 200:
            raise AssertionError(f"{path}: HTTP {response.status}: {body}")
        return body
    finally:
        connection.close()


class StorageHandoffTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)
        self.storage = self.root / "storage"
        self.storage.mkdir()
        self.coordinator = Coordinator()
        self.processes = []
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=4)

    def tearDown(self):
        self.coordinator.hold_release.set()
        self.coordinator.transaction_start_release.set()
        for process in self.processes:
            process.cleanup()
        self.executor.shutdown(wait=True, cancel_futures=True)
        self.coordinator.close()
        self.temp.cleanup()

    def start_workerd(self, generation):
        directory = self.root / generation
        directory.mkdir()
        process = WorkerdProcess(
            WORKERD_BINARY,
            directory,
            self.storage,
            self.coordinator.address,
            generation,
        )
        self.processes.append(process)
        process.start()
        return process

    def assert_queue_empty(self, values, timeout=0.2):
        with self.assertRaises(queue.Empty):
            values.get(timeout=timeout)

    def future_result(self, future, process, timeout=5):
        try:
            return future.result(timeout=timeout)
        except Exception as exception:
            self.fail(f"{exception}\n{process.read_stderr()}")

    def test_graceful_handoff_drains_old_work_without_storage(self):
        old = self.start_workerd("old")
        self.assertEqual(request(old.port, "/setup"), "started")
        self.assertTrue(self.coordinator.transaction_start_entered.wait(5))
        self.coordinator.transaction_start_release.set()
        self.assertTrue(self.coordinator.hold_entered.wait(5))

        new = self.start_workerd("new")
        self.assertEqual(request(new.port, "/plain"), "new")
        increment = self.executor.submit(request, new.port, "/increment")
        self.assert_queue_empty(self.coordinator.entered)
        self.assertFalse(increment.done())

        self.assertEqual(request(old.port, "/schedule-alarm"), "scheduled")
        old.send_signal(signal.SIGTERM)
        self.assertEqual(self.coordinator.entered.get(timeout=5), "new")
        self.assertEqual(self.future_result(increment, new), "1")
        self.assertIsNone(old.process.poll())

        self.assertEqual(self.coordinator.alarms.get(timeout=10), "new")
        self.assert_queue_empty(self.coordinator.alarms)

        self.coordinator.hold_release.set()
        report = self.coordinator.reports.get(timeout=5)
        self.assertIn("storage is no longer accessible", report)
        self.assertEqual(old.wait(timeout=10), 0, old.read_stderr())
        self.assert_queue_empty(self.coordinator.alarms)
        self.assertEqual(request(new.port, "/read"), "1")

        new.send_signal(signal.SIGTERM)
        self.assertEqual(new.wait(timeout=10), 0, new.read_stderr())

    def test_sigkill_releases_lease(self):
        old = self.start_workerd("old")
        self.assertEqual(request(old.port, "/seed"), "seeded")

        new = self.start_workerd("new")
        increment = self.executor.submit(request, new.port, "/increment")
        self.assert_queue_empty(self.coordinator.entered)
        self.assertFalse(increment.done())

        old.send_signal(signal.SIGKILL)
        self.assertEqual(old.wait(timeout=5), -signal.SIGKILL)
        self.assertEqual(self.coordinator.entered.get(timeout=5), "new")
        self.assertEqual(self.future_result(increment, new), "8")

    def test_standby_queue_is_bounded_and_cancellation_releases_slots(self):
        old = self.start_workerd("old")
        new = self.start_workerd("new")

        self.assertEqual(
            request(new.port, "/overflow?batch=first", timeout=15), "overloaded"
        )
        increment = self.executor.submit(request, new.port, "/increment")
        self.assert_queue_empty(self.coordinator.entered)
        self.assertFalse(increment.done())

        old.send_signal(signal.SIGKILL)
        self.assertEqual(old.wait(timeout=5), -signal.SIGKILL)
        self.assertEqual(self.coordinator.entered.get(timeout=5), "new")
        self.assertEqual(self.future_result(increment, new), "1")


if __name__ == "__main__":
    unittest.main()
