"""
Round-trip message delay (RTT) measurements for the chat system.

Method ("ping-pong through the server"):
  client A sends  PING:A:<id>;   -> server -> every responder client
  each responder immediately replies PONG:<name>:<id>:A;  -> server -> A
  RTT = (time the PONG reaches A) - (time A called send()), measured with one
  monotonic clock (time.perf_counter_ns) so there is no clock-sync problem.
  So one RTT covers 4 hops: A->server, server->B, B->server, server->A.

Reports per scenario: min / mean / median / p95 / max / stdev (milliseconds).

Run:  python3 tests/test_rtt.py
Output: tests/results/rtt_samples.csv, rtt_summary.csv, rtt_log.txt, fig*.png
"""
import csv
import socket
import statistics
import threading
import time

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from common import HOST, RESULTS, P, DelayProxy, Logger, ServerProc, TestClient

log = Logger()
samples_rows = []   # scenario, sample, first_ms, last_ms
summary = {}        # scenario -> stats dict
raw = {}            # scenario -> (first list, last list)


def stats(xs):
    xs = sorted(xs)
    return {
        "n": len(xs),
        "min": xs[0],
        "mean": statistics.fmean(xs),
        "median": statistics.median(xs),
        "p95": xs[min(len(xs) - 1, int(round(0.95 * (len(xs) - 1))))],
        "max": xs[-1],
        "stdev": statistics.pstdev(xs),
    }


def report(name, first, last):
    raw[name] = (first, last)
    summary[name] = {"first_reply": stats(first), "all_replies": stats(last)}
    for i, (f, l) in enumerate(zip(first, last)):
        samples_rows.append((name, i, f, l))
    s = summary[name]["all_replies"]
    log(f"{name:34s} n={s['n']:4d}  min={s['min']:7.3f}  mean={s['mean']:7.3f}  median={s['median']:7.3f}  "
        f"p95={s['p95']:7.3f}  max={s['max']:7.3f}  sd={s['stdev']:6.3f}  (ms)")


def measure(port, n_clients, samples, warmup=20, flood=False, timeout=5.0):
    a = TestClient("A", port)
    responders = [TestClient(f"R{i}", port, responder=True) for i in range(1, n_clients)]
    flooder, stop = None, threading.Event()
    if flood:
        flooder = TestClient("F", port)

        def spam():
            k = 0
            while not stop.is_set():
                k += 1
                flooder.send(f"MSG:F:{k};")
                time.sleep(0.001)
        threading.Thread(target=spam, daemon=True).start()
    time.sleep(0.4)
    first, last, lost = [], [], 0
    for i in range(warmup + samples):
        t0 = a.send(f"PING:A:{i};")
        if not a.wait_pongs(i, n_clients - 1, timeout):
            lost += 1
            continue
        times = [t for _, t in a.pongs[i]]
        if i >= warmup:
            first.append((min(times) - t0) / 1e6)
            last.append((max(times) - t0) / 1e6)
        time.sleep(0.005)          # keep pings from overlapping
    stop.set()
    for c in [a] + responders + ([flooder] if flooder else []):
        c.close()
    if lost:
        log(f"   ({lost} pings got no complete reply within {timeout}s)")
    return first, last


def baseline_echo(samples, warmup=20):
    """Floor: A <-> plain TCP echo on localhost, no chat server (2 hops)."""
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((HOST, 0))
    srv.listen()
    port = srv.getsockname()[1]

    def echo():
        c, _ = srv.accept()
        c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        while True:
            d = c.recv(4096)
            if not d:
                return
            c.sendall(d)
    threading.Thread(target=echo, daemon=True).start()
    c = socket.create_connection((HOST, port))
    c.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    out = []
    for i in range(warmup + samples):
        t0 = time.perf_counter_ns()
        c.sendall(b"x" * 24)
        c.recv(4096)
        t = (time.perf_counter_ns() - t0) / 1e6
        if i >= warmup:
            out.append(t)
        time.sleep(0.005)
    c.close()
    srv.close()
    return out, out


if __name__ == "__main__":
    log("RTT = A sends PING -> server -> responders -> PONG -> server -> A (4 hops through the server)")
    log("Times in milliseconds. 'all replies' = until the LAST responder's PONG arrived.")
    log()
    log("--- Experiment 1: baseline (plain TCP echo, no chat server, 2 hops) ---")
    f, l = baseline_echo(300)
    report("baseline_tcp_echo", f, l)

    log("--- Experiment 2: 2 clients and 3 clients, server as written (the lab requirement) ---")
    srv = ServerProc(P(9201))
    f, l = measure(P(9201), 2, 300); report("chat_2_clients", f, l)
    srv.stop()
    srv = ServerProc(P(9202))
    f, l = measure(P(9202), 3, 300); report("chat_3_clients", f, l)
    srv.stop()

    log("--- Experiment 3: scaling the number of clients, server as written ---")
    NS = (2, 3, 5, 10, 20)
    for n in NS:
        srv = ServerProc(P(9210 + n))
        f, l = measure(P(9210 + n), n, 150); report(f"scale_{n:02d}_clients", f, l)
        srv.stop()

    log("--- Experiment 4: same server + TCP_NODELAY (tests the Nagle / delayed-ACK hypothesis) ---")
    srv = ServerProc(P(9602), nodelay=True)
    f, l = measure(P(9602), 3, 300); report("nodelay_3_clients", f, l)
    srv.stop()
    for n in NS:
        srv = ServerProc(P(9610 + n), nodelay=True)
        f, l = measure(P(9610 + n), n, 150); report(f"nodelay_scale_{n:02d}_clients", f, l)
        srv.stop()

    log("--- Experiment 5: injected network delay, 3 clients (delay added per link, each direction) ---")
    delays = (0, 10, 25, 50)
    for tag, nd in (("delay", False), ("nodelay_delay", True)):
        for d in delays:
            srv = ServerProc(P(9301 + d + (1000 if nd else 0)), nodelay=nd)
            proxy = DelayProxy(P(9401 + d + (1000 if nd else 0)), P(9301 + d + (1000 if nd else 0)), d)
            f, l = measure(P(9401 + d + (1000 if nd else 0)), 3, 40 if d else 100, warmup=5)
            report(f"{tag}_{d:02d}ms_per_link", f, l)
            proxy.stop(); srv.stop()

    log("--- Experiment 6: RTT under background load (3 clients + 1 flooder, ~1000 msg/s) ---")
    srv = ServerProc(P(9501))
    f, l = measure(P(9501), 3, 200, flood=True); report("chat_3_clients_under_load", f, l)
    srv.stop()
    srv = ServerProc(P(9502), nodelay=True)
    f, l = measure(P(9502), 3, 200, flood=True); report("nodelay_3_clients_under_load", f, l)
    srv.stop()

    # ---------------- save ----------------
    with open(f"{RESULTS}/rtt_samples.csv", "w", newline="") as fh:
        w = csv.writer(fh); w.writerow(["scenario", "sample", "first_reply_ms", "all_replies_ms"]); w.writerows(
            (a, b, f"{c:.4f}", f"{d:.4f}") for a, b, c, d in samples_rows)
    with open(f"{RESULTS}/rtt_summary.csv", "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["scenario", "metric", "n", "min_ms", "mean_ms", "median_ms", "p95_ms", "max_ms", "stdev_ms"])
        for sc, d in summary.items():
            for metric, s in d.items():
                w.writerow([sc, metric, s["n"]] + [f"{s[k]:.4f}" for k in ("min", "mean", "median", "p95", "max", "stdev")])
    log.save("rtt_log.txt")

    # ---------------- charts ----------------
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})
    BLUE, RED, GREEN = "#1f77b4", "#d62728", "#2ca02c"
    mean = lambda k: summary[k]["all_replies"]["mean"]
    p95 = lambda k: summary[k]["all_replies"]["p95"]

    # Fig 1: RTT per message
    fig, ax = plt.subplots(1, 2, figsize=(11, 3.8))
    series = (("chat_2_clients", "2 clients", BLUE), ("chat_3_clients", "3 clients", RED),
              ("nodelay_3_clients", "3 clients + TCP_NODELAY", GREEN))
    for name, label, col in series:
        ax[0].plot(raw[name][1], lw=0.8, label=label, color=col, alpha=0.85)
    ax[0].set_yscale("log"); ax[0].set_xlabel("message number"); ax[0].set_ylabel("round-trip time (ms, log scale)")
    ax[0].set_title("RTT per message"); ax[0].legend(fontsize=8)
    bins = [10 ** (x / 20) for x in range(-24, 42)]
    for name, label, col in series:
        ax[1].hist(raw[name][1], bins=bins, alpha=0.6, label=label, color=col)
    ax[1].set_xscale("log"); ax[1].set_xlabel("round-trip time (ms, log scale)"); ax[1].set_ylabel("count")
    ax[1].set_title("RTT distribution"); ax[1].legend(fontsize=8)
    fig.tight_layout(); fig.savefig(f"{RESULTS}/fig1_rtt_2_vs_3_clients.png", dpi=160); plt.close(fig)

    # Fig 2: scaling
    fig, ax = plt.subplots(figsize=(6.8, 4))
    ax.plot(NS, [mean(f"scale_{n:02d}_clients") for n in NS], "o-", color=RED, label="server as written (mean)")
    ax.plot(NS, [p95(f"scale_{n:02d}_clients") for n in NS], "o--", color=RED, alpha=0.5, label="server as written (p95)")
    ax.plot(NS, [mean(f"nodelay_scale_{n:02d}_clients") for n in NS], "s-", color=GREEN, label="with TCP_NODELAY (mean)")
    ax.plot(NS, [p95(f"nodelay_scale_{n:02d}_clients") for n in NS], "s--", color=GREEN, alpha=0.5, label="with TCP_NODELAY (p95)")
    ax.set_yscale("log"); ax.set_xticks(NS)
    ax.set_xlabel("number of connected clients"); ax.set_ylabel("round-trip time, all replies (ms, log scale)")
    ax.set_title("RTT vs number of clients"); ax.legend(fontsize=8)
    fig.tight_layout(); fig.savefig(f"{RESULTS}/fig2_rtt_vs_clients.png", dpi=160); plt.close(fig)

    # Fig 3: injected delay
    base = mean("nodelay_delay_00ms_per_link")
    fig, ax = plt.subplots(figsize=(6.8, 4))
    ax.plot(delays, [mean(f"delay_{d:02d}ms_per_link") for d in delays], "o-", color=RED, label="server as written")
    ax.plot(delays, [mean(f"nodelay_delay_{d:02d}ms_per_link") for d in delays], "s-", color=GREEN, label="with TCP_NODELAY")
    ax.plot(delays, [base + 4 * d for d in delays], "k--", lw=1, label="model: baseline + 4 x delay")
    ax.set_xlabel("injected one-way delay per link (ms)"); ax.set_ylabel("mean round-trip time (ms)")
    ax.set_xticks(delays); ax.set_title("RTT vs injected network delay (3 clients)"); ax.legend(fontsize=8)
    fig.tight_layout(); fig.savefig(f"{RESULTS}/fig3_rtt_vs_injected_delay.png", dpi=160); plt.close(fig)

    # Fig 4: idle vs load
    fig, ax = plt.subplots(figsize=(7, 4))
    data = [raw["chat_3_clients"][1], raw["chat_3_clients_under_load"][1],
            raw["nodelay_3_clients"][1], raw["nodelay_3_clients_under_load"][1]]
    ax.boxplot(data, tick_labels=["as written\nidle", "as written\nunder load", "NODELAY\nidle", "NODELAY\nunder load"])
    ax.set_yscale("log"); ax.set_ylabel("round-trip time (ms, log scale)")
    ax.set_title("RTT with 3 clients: idle vs background load (~1000 msg/s)")
    fig.tight_layout(); fig.savefig(f"{RESULTS}/fig4_rtt_idle_vs_load.png", dpi=160); plt.close(fig)
    print("done")
