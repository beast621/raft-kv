#!/usr/bin/env python3
"""Sequential, real TCP, majority durable writes; verify after full restart."""
import argparse
import json
from pathlib import Path
import statistics
import sys
import time
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "tests"))
from integration import Cluster, request, command

def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered)-1, int((len(ordered)-1)*fraction))]

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("binary")
    parser.add_argument("--operations", type=int, default=50)
    parser.add_argument("--rounds", type=int, default=3)
    args = parser.parse_args()
    if not 1 <= args.operations <= 500 or not 1 <= args.rounds <= 10:
        parser.error("operations 1..500, rounds 1..10")
    rounds = []
    for number in range(args.rounds):
        c = Cluster(str(Path(args.binary).resolve()), snapshot_every=32)
        try:
            leader = c.leader()
            samples = []
            start = time.perf_counter()
            for seq in range(1, args.operations+1):
                before = time.perf_counter()
                out = request(c.ports[leader+2], command(1,f"bench-{seq:04}","v"*64,"bench",seq))
                if out["status"] != "OK":
                    raise RuntimeError(f"benchmark interrupted (no hidden retry): {out}")
                samples.append((time.perf_counter()-before)*1000)
            elapsed = time.perf_counter()-start
            for i in list(c.processes): c.kill(i)
            for i in range(1,4): c.start(i)
            c.leader()
            for seq in range(1,args.operations+1):
                if c.call(3,f"bench-{seq:04}")["value"] != "v"*64:
                    raise AssertionError("restart verification failed")
            rounds.append({
                "round": number+1, "operations": args.operations, "value_bytes":64,
                "seconds":elapsed, "writes_per_second":args.operations/elapsed,
                "p50_ms":percentile(samples,.50), "p95_ms":percentile(samples,.95),
                "p99_ms":percentile(samples,.99), "restart_verified":True
            })
        finally:
            c.close()
    result = {"scope":"3 processes, TCP connection per operation, sequential writes, majority fsync, snapshot every 32",
              "excludes":"startup/election, full restart and verification",
              "rounds":rounds,
              "median_writes_per_second":statistics.median(r["writes_per_second"] for r in rounds)}
    print(json.dumps(result, indent=2))
if __name__ == "__main__": main()
