import http.client
import json
import os
import select
import signal
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

WORKERD_BINARY, WORKERD_CONFIG = sys.argv[1:3]
del sys.argv[1:3]


class AdminFdTest(unittest.TestCase):
    def expect_usage_error(self, args, expected, pass_fds=()):
        result = subprocess.run(
            [WORKERD_BINARY, "serve", *args, WORKERD_CONFIG],
            pass_fds=pass_fds,
            capture_output=True,
            text=True,
            timeout=10,
        )
        self.assertNotEqual(result.returncode, 0)
        self.assertIn(expected, result.stderr)
        self.assertNotIn("*** Uncaught exception ***", result.stderr)

    def test_unix_stream_survives_admin_eof(self):
        admin_parent, admin_child = socket.socketpair()
        control_read, control_write = os.pipe()
        process = subprocess.Popen(
            [
                WORKERD_BINARY,
                "serve",
                WORKERD_CONFIG,
                "--socket-addr=http=127.0.0.1:0",
                f"--control-fd={control_write}",
                f"--admin-fd={admin_child.fileno()}",
            ],
            pass_fds=(admin_child.fileno(), control_write),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
        )
        admin_child.close()
        os.close(control_write)

        try:
            readable, _, _ = select.select([control_read], [], [], 10)
            self.assertEqual(readable, [control_read])
            message = json.loads(os.read(control_read, 4096).splitlines()[0])
            self.assertEqual(message["event"], "listen")
            self.assertEqual(message["socket"], "http")

            admin_parent.close()
            time.sleep(0.1)
            self.assertIsNone(process.poll())

            connection = http.client.HTTPConnection(
                "127.0.0.1", message["port"], timeout=5
            )
            connection.request("GET", "/")
            response = connection.getresponse()
            self.assertEqual(response.status, 200)
            response.read()
            connection.close()
        finally:
            admin_parent.close()
            os.close(control_read)
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

        stderr = process.stderr.read()
        process.stderr.close()
        self.assertIn(process.returncode, (0, -signal.SIGTERM), stderr)

    def test_rejects_tcp_and_non_stream_sockets(self):
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        tcp_client = socket.create_connection(listener.getsockname())
        tcp_server, _ = listener.accept()
        with listener, tcp_client, tcp_server:
            self.expect_usage_error(
                [f"--admin-fd={tcp_server.fileno()}"],
                "connected Unix stream socket",
                (tcp_server.fileno(),),
            )

        datagram_parent, datagram_child = socket.socketpair(type=socket.SOCK_DGRAM)
        with datagram_parent, datagram_child:
            self.expect_usage_error(
                [f"--admin-fd={datagram_child.fileno()}"],
                "connected Unix stream socket",
                (datagram_child.fileno(),),
            )

    def test_rejects_listening_and_non_socket_descriptors(self):
        with tempfile.TemporaryDirectory() as directory:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as listener:
                listener.bind(str(Path(directory) / "admin.sock"))
                listener.listen()
                self.expect_usage_error(
                    [f"--admin-fd={listener.fileno()}"],
                    "connected Unix stream socket",
                    (listener.fileno(),),
                )

        with Path(os.devnull).open("rb") as not_a_socket:
            self.expect_usage_error(
                [f"--admin-fd={not_a_socket.fileno()}"],
                "connected Unix stream socket",
                (not_a_socket.fileno(),),
            )

    def test_rejects_out_of_range_descriptor(self):
        self.expect_usage_error(
            ["--admin-fd=2147483648"],
            "file descriptor (non-negative integer)",
        )

    def test_rejects_watch_in_both_orders(self):
        admin_parent, admin_child = socket.socketpair()
        with admin_parent, admin_child:
            fd = admin_child.fileno()
            self.expect_usage_error(
                [f"--admin-fd={fd}", "--watch"],
                "--watch cannot be combined with --admin-fd",
                (fd,),
            )
            self.expect_usage_error(
                ["--watch", f"--admin-fd={fd}"],
                "--admin-fd cannot be combined with --watch",
                (fd,),
            )


if __name__ == "__main__":
    unittest.main()
