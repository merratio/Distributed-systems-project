"""
Shared helpers for the chat-system test suite.

TestClient   - a scriptable client that speaks the same wire format as client/client.py
               ("[HH:MM:SS] name: text") and records when every message arrives.
start_server - launches server/server_fixed.py as a separate process on a chosen port.
DelayProxy   - a TCP proxy that adds a fixed one-way delay (used to simulate a slow network).
"""
import os
import queue
import re
import socket
import struct
import subprocess
import sys
import threading
import time
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.abspath(os.path.join(HERE, ".."))
SERVER_DIR = os.path.join(ROOT, "server")
CLIENT_PY = os.path.join(ROOT, "client", "client.py")
RESULTS = os.path.join(HERE, "results")
os.makedirs(RESULTS, exist_ok=True)

HOST = "127.0.0.1"

# The server has no SO_REUSEADDR, so re-running on the same port within ~60 s fails with
# "address already in use". Every run therefore uses a fresh random port range.
import random
_OFFSET = random.randint(0, 200) * 100


def P(port):
    return port + _OFFSET

# Tokens we embed in message text so the receiver can recognise them even if TCP
# glues several chat messages together (the protocol has no message delimiter).
TOKEN_RE = re.compile(r"(MSG|PING|PONG):([A-Za-z0-9_]+):(\d+)(?::([A-Za-z0-9_]+))?;")


class Logger:
    def __init__(self):
        self.lines = []
        self.t0 = time.time()

    def __call__(self, text=""):
        stamp = f"[{time.time() - self.t0:7.3f}s] "
        line = stamp + text
        print(line, flush=True)
        self.lines.append(line)

    def save(self, name):
        with open(os.path.join(RESULTS, name), "w") as f:
            f.write("\n".join(self.lines) + "\n")


class ServerProc:
    """The chat server running in its own process, with its console output captured."""

    def __init__(self, port, nodelay=False):
        """nodelay=True runs the SAME server code but sets TCP_NODELAY on every accepted
        connection (used only for the Nagle / delayed-ACK experiment; server_fixed.py is not edited)."""
        self.port = port
        patch = ""
        if nodelay:
            patch = ("import socket; _h = s.handleClients; "
                     "s.handleClients = lambda c, a: (c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1), _h(c, a))[1]; ")
        code = (
            f"import sys; sys.path.insert(0, {SERVER_DIR!r}); "
            f"import server_fixed as s; s.PORT = {port}; {patch}s.server()"
        )
        self.proc = subprocess.Popen(
            [sys.executable, "-u", "-c", code],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        self.port_label = port
        self.output = []
        threading.Thread(target=self._drain, daemon=True).start()
        for _ in range(100):  # wait until it accepts connections
            try:
                socket.create_connection((HOST, port), timeout=0.2).close()
                break
            except OSError:
                time.sleep(0.05)
        time.sleep(0.1)
        if self.proc.poll() is not None:
            raise RuntimeError(f"server on port {port} failed to start: {self.output}")
        self.output.clear()  # drop the probe connection's lines

    def _drain(self):
        for line in self.proc.stdout:
            self.output.append(line.rstrip())

    def alive(self):
        return self.proc.poll() is None

    def stop(self):
        self.proc.kill()
        self.proc.wait()


class TestClient:
    def __init__(self, name, port, responder=False, host=HOST):
        self.name = name
        self.responder = responder
        self.sock = socket.create_connection((host, port))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.buf = ""
        self.transcript = []      # decoded text of every recv() call
        self.recv_calls = 0
        self.msgs = []            # (kind, sender, number) in arrival order
        self.pongs = {}           # ping id -> [(responder, t_ns)]
        self.cv = threading.Condition()
        self.send_lock = threading.Lock()
        self.closed = False
        threading.Thread(target=self._rx, daemon=True).start()

    # ---- receiving -------------------------------------------------------
    def _rx(self):
        while True:
            try:
                data = self.sock.recv(4096)
            except OSError:
                break
            if not data:
                break
            t = time.perf_counter_ns()
            text = data.decode("utf-8", errors="replace")
            self.recv_calls += 1
            self.transcript.append(text)
            self.buf += text
            last_end = 0
            for m in TOKEN_RE.finditer(self.buf):
                last_end = m.end()
                kind, who, num, extra = m.group(1), m.group(2), int(m.group(3)), m.group(4)
                if kind == "PING" and self.responder and who != self.name:
                    self.send(f"PONG:{self.name}:{num}:{who};")
                elif kind == "PONG" and extra == self.name:
                    with self.cv:
                        self.pongs.setdefault(num, []).append((who, t))
                        self.cv.notify_all()
                with self.cv:
                    self.msgs.append((kind, who, num))
            self.buf = self.buf[last_end:]
            if len(self.buf) > 20000:
                self.buf = self.buf[-200:]
        self.closed = True

    def wait_pongs(self, ping_id, count, timeout=3.0):
        with self.cv:
            return self.cv.wait_for(lambda: len(self.pongs.get(ping_id, [])) >= count, timeout)

    def seqs_from(self, sender, kind="MSG"):
        with self.cv:
            return [n for k, w, n in self.msgs if k == kind and w == sender]

    # ---- sending ---------------------------------------------------------
    def send(self, token):
        """Send one chat line in the same format as client.py. Returns send time (ns)."""
        stamp = datetime.now().strftime("%H:%M:%S")
        payload = f"[{stamp}] {self.name}: {token}".encode("utf-8")
        with self.send_lock:
            t = time.perf_counter_ns()
            self.sock.sendall(payload)
        return t

    def send_raw(self, text):
        with self.send_lock:
            self.sock.sendall(text.encode("utf-8"))

    # ---- closing ---------------------------------------------------------
    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass

    def crash(self):
        """Abrupt disconnect (TCP RST) - simulates a process crash / cable pull."""
        try:
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            self.sock.close()
        except OSError:
            pass


class DelayProxy:
    """Forwards TCP traffic and delays every chunk by `delay_ms` in each direction."""

    def __init__(self, listen_port, target_port, delay_ms):
        self.delay = delay_ms / 1000.0
        self.target_port = target_port
        self.srv = socket.socket()
        self.srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.srv.bind((HOST, listen_port))
        self.srv.listen()
        threading.Thread(target=self._accept, daemon=True).start()

    def _accept(self):
        while True:
            try:
                c, _ = self.srv.accept()
            except OSError:
                return
            s = socket.create_connection((HOST, self.target_port))
            for sock in (c, s):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._pump(c, s)
            self._pump(s, c)

    def _pump(self, src, dst):
        q = queue.Queue()

        def reader():
            while True:
                try:
                    data = src.recv(4096)
                except OSError:
                    data = b""
                q.put((time.monotonic() + self.delay, data))
                if not data:
                    return

        def writer():
            while True:
                due, data = q.get()
                wait = due - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
                if not data:
                    for x in (src, dst):
                        try:
                            x.close()
                        except OSError:
                            pass
                    return
                try:
                    dst.sendall(data)
                except OSError:
                    return

        threading.Thread(target=reader, daemon=True).start()
        threading.Thread(target=writer, daemon=True).start()

    def stop(self):
        try:
            self.srv.close()
        except OSError:
            pass
