# Testing & message-delay suite

Requires Python 3.8+ and (for the charts only) `matplotlib`. Everything runs on localhost.

    python3 tests/test_functional.py   # messaging, ordering, concurrency, disconnect, reconnect, real client.py  (~20 s)
    python3 tests/test_rtt.py          # round-trip delay experiments + charts                                   (~2 min)

Results are written to `tests/results/` (logs, CSV, PNG charts).

The tests start `server/server_fixed.py` (a minimal Python 3 fix of `server/server.py`, see the 3 `FIX` comments)
as a separate process on a random free port range, so nothing needs to be started by hand and re-runs do not
collide (the server does not set SO_REUSEADDR). `client/client.py` is used unmodified in test T7.

Files: `common.py` (test client, server launcher, delay proxy) | `test_functional.py` | `test_rtt.py`
