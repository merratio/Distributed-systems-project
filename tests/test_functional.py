"""
Functional tests for the chat system: messaging, ordering, concurrency,
disconnection, reconnection, and an end-to-end run with the real client.py.

Run:  python3 tests/test_functional.py
Output: console + tests/results/functional_log.txt + functional_results.json
"""
import json
import os
import re
import subprocess
import sys
import threading
import time

from common import (CLIENT_PY, HOST, RESULTS, P, Logger, ServerProc, TestClient)

log = Logger()
results = []


def record(test_id, name, passed, detail, expected_limitation=False, defect=False):
    status = "PASS" if passed else ("LIMITATION" if expected_limitation else ("DEFECT" if defect else "FAIL"))
    results.append({"id": test_id, "name": name, "status": status, "detail": detail})
    log(f"RESULT {test_id} {name}: {status} - {detail}")
    log()


def settle(sec=0.4):
    time.sleep(sec)


# --------------------------------------------------------------------------
def t1_broadcast():
    log("=== T1: 3 clients - broadcast reaches everyone except the sender ===")
    srv = ServerProc(P(9101))
    a, b, c = (TestClient(n, P(9101)) for n in "ABC")
    settle()
    a.send("MSG:A:1;")
    b.send("MSG:B:1;")
    c.send("MSG:C:1;")
    settle()
    log("Server console:")
    for line in srv.output:
        log("   " + line)
    for cl in (a, b, c):
        log(f"Client {cl.name} received: {''.join(cl.transcript)!r}")
    ok = (
        a.seqs_from("A") == [] and b.seqs_from("B") == [] and c.seqs_from("C") == []
        and a.seqs_from("B") == [1] and a.seqs_from("C") == [1]
        and b.seqs_from("A") == [1] and b.seqs_from("C") == [1]
        and c.seqs_from("A") == [1] and c.seqs_from("B") == [1]
    )
    record("T1", "Broadcast to all other clients, no echo to sender", ok,
           "each client received exactly the 2 messages sent by the other 2 clients and none of its own")
    for cl in (a, b, c):
        cl.close()
    srv.stop()


# --------------------------------------------------------------------------
def t2_ordering():
    log("=== T2: ordering of messages from one sender ===")
    srv = ServerProc(P(9102))
    a, b, c = (TestClient(n, P(9102)) for n in "ABC")
    settle()
    N = 100
    for i in range(1, N + 1):
        a.send(f"MSG:A:{i};")
        time.sleep(0.002)
    settle(0.6)
    ok_paced = b.seqs_from("A") == list(range(1, N + 1)) and c.seqs_from("A") == list(range(1, N + 1))
    record("T2a", "100 paced messages arrive complete and in order at both receivers", ok_paced,
           f"B got {len(b.seqs_from('A'))}/100, C got {len(c.seqs_from('A'))}/100")

    # burst: no delay between sends
    b.msgs.clear(); c.msgs.clear(); b.recv_calls = 0; c.recv_calls = 0
    M = 1000
    for i in range(1, M + 1):
        a.send(f"MSG:A:{i};")
    settle(1.0)
    got_b, got_c = b.seqs_from("A"), c.seqs_from("A")
    ok_burst = got_b == list(range(1, M + 1)) and got_c == list(range(1, M + 1))
    missing = sorted(set(range(1, M + 1)) - set(got_b))
    full = "".join(b.transcript)
    mid = [m.start() for m in re.finditer(r"\('127\.0\.0\.1', \d+\): ", full)
           if full[max(0, m.start() - 1):m.start()] not in (";", "")]
    in_order = got_b == sorted(got_b)
    record("T2b", "1000-message burst arrives complete", ok_burst,
           f"B got {len(got_b)}/{M}; missing message numbers {missing}. Cause: server recv(1024) cut messages at "
           f"the 1024-byte boundary and inserted the sender-address prefix mid-message "
           f"({len(mid)} prefixes found inside a message). Messages that did arrive were in order: {in_order}",
           defect=True)
    log(f"Observation: B needed only {b.recv_calls} recv() calls for {M} messages "
        f"(TCP merged several chat lines into one read - there is no message delimiter).")
    if mid:
        i = mid[0]
        log(f"Example of corrupted stream at B: ...{full[max(0, i - 22):i]!r} <PREFIX INSERTED> {full[i:i + 40]!r}...")
    results.append({"id": "T2b-obs", "name": "TCP coalescing during burst", "status": "INFO",
                    "detail": f"{M} messages delivered in {b.recv_calls} recv() calls at client B"})
    for cl in (a, b, c):
        cl.close()
    srv.stop()


# --------------------------------------------------------------------------
def run_concurrent(port, pace, label):
    srv = ServerProc(port)
    senders = [TestClient(n, port) for n in "ABC"]
    listeners = [TestClient("D", port), TestClient("E", port)]
    settle()
    N = 100

    def blast(cl):
        for i in range(1, N + 1):
            cl.send(f"MSG:{cl.name}:{i};")
            if pace:
                time.sleep(pace)

    threads = [threading.Thread(target=blast, args=(s,)) for s in senders]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    settle(1.2)

    received = {cl.name: sum(len(cl.seqs_from(s.name)) for s in senders) for cl in listeners}
    complete = all(len(cl.seqs_from(s.name)) == N for cl in listeners for s in senders)
    fifo = all(cl.seqs_from(s.name) == sorted(cl.seqs_from(s.name))
               for cl in listeners + senders for s in senders if s is not cl)
    d, e = listeners
    # compare the global interleaving only over messages BOTH listeners received
    common = set((w, n) for k, w, n in d.msgs) & set((w, n) for k, w, n in e.msgs)
    seq_d = [(w, n) for k, w, n in d.msgs if (w, n) in common]
    seq_e = [(w, n) for k, w, n in e.msgs if (w, n) in common]
    same_order = seq_d == seq_e
    return srv, senders + listeners, received, complete, fifo, same_order, len(common)


def t3_concurrent():
    log("=== T3: concurrent senders (3 senders x 100 msgs + 2 silent listeners) ===")
    # (i) paced: 5 ms between sends, i.e. normal chat-like load
    srv, cls, received, complete, fifo, same, ncommon = run_concurrent(P(9103), 0.005, "paced")
    record("T3a", "3 concurrent senders, moderate load: no message lost", complete,
           f"listeners D/E received {received} of 300 messages")
    record("T3b", "Per-sender (FIFO) order preserved", fifo, "each receiver saw every sender's messages in increasing order")
    record("T3c", "All receivers see the same global interleaving", same,
           f"D and E agree on the order of all {ncommon} messages they both received "
           "(server broadcasts while holding one lock, so it acts as a sequencer)")
    for c in cls:
        c.close()
    srv.stop()
    # (ii) unpaced burst
    srv, cls, received, complete, fifo, same, ncommon = run_concurrent(P(9107), 0, "burst")
    record("T3d", "3 concurrent senders, burst load (no pacing): no message lost", complete,
           f"listeners D/E received {received} of 300 messages (same 1024-byte framing defect as T2b)",
           defect=True)
    record("T3e", "Burst load: per-sender order and global order still consistent", fifo and same,
           f"FIFO per sender: {fifo}; D and E agree on order of {ncommon} common messages: {same}")
    for c in cls:
        c.close()
    srv.stop()


# --------------------------------------------------------------------------
def t4_disconnect():
    log("=== T4: client disconnections (clean and abrupt) ===")
    srv = ServerProc(P(9104))
    a, b, c, d = (TestClient(n, P(9104)) for n in "ABCD")
    settle()
    c.close()                       # clean close (FIN)
    settle()
    a.send("MSG:A:1;")
    settle()
    ok_clean = (b.seqs_from("A") == [1] and d.seqs_from("A") == [1] and srv.alive())
    record("T4a", "Clean disconnect of one client: others keep chatting, server stays up", ok_clean,
           "B and D still receive A's message after C left")

    d.crash()                       # abrupt close (RST)
    settle()
    a.send("MSG:A:2;")
    b.send("MSG:B:1;")
    settle()
    ok_abrupt = (b.seqs_from("A") == [1, 2] and a.seqs_from("B") == [1] and srv.alive())
    record("T4b", "Abrupt disconnect (connection reset): server survives, A and B still chat", ok_abrupt,
           "A->B and B->A delivery continues after D's connection was reset")
    log("Server console:")
    for line in srv.output:
        log("   " + line)
    for cl in (a, b):
        cl.close()
    srv.stop()


# --------------------------------------------------------------------------
def t5_reconnect():
    log("=== T5: reconnection and offline messages ===")
    srv = ServerProc(P(9105))
    a, b, c = (TestClient(n, P(9105)) for n in "ABC")
    settle()
    a.send("MSG:A:1;")
    settle()
    c.close()
    settle()
    for i in range(2, 7):            # 5 messages while C is offline
        a.send(f"MSG:A:{i};")
        time.sleep(0.01)
    settle()
    c2 = TestClient("C", P(9105))       # C reconnects (new TCP connection)
    settle()
    for i in range(7, 12):           # 5 messages after C is back
        a.send(f"MSG:A:{i};")
        time.sleep(0.01)
    settle()
    got = c2.seqs_from("A")
    ok_reconnect = got == [7, 8, 9, 10, 11]
    record("T5a", "Client can reconnect and receive new messages", ok_reconnect,
           f"reconnected C received A's messages {got}")
    missed = [2, 3, 4, 5, 6]
    record("T5b", "Messages sent while a client was offline are delivered after reconnect",
           got[:5] == missed, "messages 2-6 were NOT delivered: the server has no per-client buffer "
           "(optional 'persistent communication' extension is not implemented)",
           expected_limitation=True)
    record("T5c", "Always-online client B received everything", b.seqs_from("A") == list(range(1, 12)),
           "B received messages 1-11")
    log("Server console:")
    for line in srv.output:
        log("   " + line)
    for cl in (a, b, c2):
        cl.close()
    srv.stop()


# --------------------------------------------------------------------------
def t6_edge_cases():
    log("=== T6: edge cases ===")
    srv = ServerProc(P(9106))
    a, b = TestClient("A", P(9106)), TestClient("B", P(9106))
    settle()
    big = "x" * 3000
    a.send_raw("[00:00:00] A: " + big)
    settle(0.6)
    chunks = [len(t) for t in b.transcript]
    prefix_count = sum(t.count("('127.0.0.1',") for t in b.transcript)
    log(f"3014-byte message from A reached B with the sender prefix ('ip', port): inserted {prefix_count} times "
        f"(server split it into {prefix_count} pieces of <=1024 bytes; B's recv() calls returned sizes {chunks})")
    results.append({"id": "T6a", "name": "Message longer than 1024 bytes", "status": "LIMITATION",
                    "detail": f"server reads at most 1024 bytes per recv() and re-broadcasts each piece with its own "
                              f"address prefix: the 3014-byte message arrived as {prefix_count} pieces, each with its own prefix"})
    log("RESULT T6a Long message (>1024 bytes) is split into prefixed pieces: LIMITATION")
    log()

    # empty / whitespace
    a.send_raw(" ")
    settle()
    log(f"Whitespace-only message delivered to B as: {b.transcript[-1]!r}")
    for cl in (a, b):
        cl.close()
    srv.stop()


# --------------------------------------------------------------------------
def t7_real_client():
    log("=== T7: end-to-end with the real client/client.py (2 clients) ===")
    srv = ServerProc(P(8000))

    def start(name):
        code = (f"import sys; sys.path.insert(0, {os.path.dirname(CLIENT_PY)!r}); import client; "
                f"client.PORT = {P(8000)}; client.main()")   # client.py itself is run unmodified
        p = subprocess.Popen([sys.executable, "-u", "-c", code], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        out = []
        threading.Thread(target=lambda: [out.append(l.rstrip("\n")) for l in p.stdout], daemon=True).start()
        p.stdin.write(name + "\n"); p.stdin.flush()
        return p, out

    alice, a_out = start("Alice")
    bob, b_out = start("Bob")
    time.sleep(1.0)
    alice.stdin.write("hello Bob\n"); alice.stdin.flush(); time.sleep(0.5)
    bob.stdin.write("hi Alice\n"); bob.stdin.flush(); time.sleep(0.5)
    alice.stdin.write("/quit\n"); alice.stdin.flush(); time.sleep(0.6)
    bob.stdin.write("anyone still here?\n"); bob.stdin.flush(); time.sleep(0.5)

    log("--- Alice's terminal ---")
    for l in a_out:
        log("   " + l)
    log("--- Bob's terminal ---")
    for l in b_out:
        log("   " + l)
    log("--- Server console ---")
    for l in srv.output:
        log("   " + l)

    bob_saw_hello = any("Alice: hello Bob" in l for l in b_out)
    alice_saw_hi = any("Bob: hi Alice" in l for l in a_out)
    alice_not_echo = not any("Alice: hello Bob" in l for l in a_out)
    has_timestamp = any(("[" in l and "] Alice:" in l) for l in b_out)
    record("T7a", "Real clients exchange messages with timestamps", bob_saw_hello and alice_saw_hi and has_timestamp,
           "Bob received '[HH:MM:SS] Alice: hello Bob'; Alice received Bob's reply")
    record("T7b", "Sender does not receive its own message", alice_not_echo, "Alice's terminal does not show her own line")
    left = any("has disconnected" in l for l in srv.output)
    record("T7c", "/quit makes the server log the departure", left, "server printed 'Client ... has disconnected'")

    # server crash
    srv.stop()
    time.sleep(0.8)
    bob_notice = any("Server disconnected" in l for l in b_out)
    record("T7d", "Client notices when the server goes down", bob_notice,
           "Bob's terminal printed 'Server disconnected.'")
    bob.stdin.write("\n"); bob.stdin.flush(); time.sleep(0.6)
    exited = bob.poll() is not None
    log(f"Bob's client exited after pressing Enter: {exited}")
    for p in (alice, bob):
        try:
            p.kill()
        except Exception:
            pass


if __name__ == "__main__":
    for t in (t1_broadcast, t2_ordering, t3_concurrent, t4_disconnect, t5_reconnect, t6_edge_cases, t7_real_client):
        t()
    log("=== SUMMARY ===")
    for r in results:
        log(f"{r['id']:7s} {r['status']:10s} {r['name']}")
    log.save("functional_log.txt")
    with open(f"{RESULTS}/functional_results.json", "w") as f:
        json.dump(results, f, indent=2)
