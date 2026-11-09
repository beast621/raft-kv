#!/usr/bin/env python3
"""Three real processes, real TCP and ext4 temporary directories."""
import concurrent.futures
import json
import os
from pathlib import Path
import random
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

BINARY = str(Path(sys.argv[1]).resolve()) if len(sys.argv) > 1 else ""
def number(n): return struct.pack("!Q", n)
def text(s):
    if isinstance(s, str): s = s.encode()
    return number(len(s)) + s
def command(op, key="", value="", client="test", seq=1):
    return number(op) + text(client) + number(seq) + text(key) + text(value)
def exact(s, n):
    out = b""
    while len(out) < n:
        part = s.recv(n - len(out))
        if not part: raise EOFError("partial response")
        out += part
    return out
def request(port, payload, fragmented=False):
    with socket.create_connection(("127.0.0.1", port), timeout=5) as s:
        s.settimeout(5)
        packet = struct.pack("!I", len(payload)) + payload
        if fragmented:
            for at in range(0, len(packet), 3): s.sendall(packet[at:at+3])
        else: s.sendall(packet)
        size = struct.unpack("!I", exact(s, 4))[0]
        reply = exact(s, size)
    pos = 0
    fields = []
    for _ in range(3):
        n = struct.unpack_from("!Q", reply, pos)[0]; pos += 8
        fields.append(reply[pos:pos+n].decode()); pos += n
    assert pos == len(reply)
    return json.loads(fields[2])

class Cluster:
    def __init__(self, binary=BINARY, snapshot_every=8):
        self.binary = binary
        base = Path(__file__).resolve().parents[1] / ".validation-tmp"
        base.mkdir(exist_ok=True)
        self.temp = tempfile.TemporaryDirectory(prefix="cluster-", dir=base)
        self.root = Path(self.temp.name)
        self.processes = {}
        self.logs = {}
        self.snapshot_every = snapshot_every
        # Reserve all endpoints while choosing them; release just before launching.
        reservations = []
        for _ in range(6):
            s = socket.socket(); s.bind(("127.0.0.1", 0)); reservations.append(s)
        self.ports = [s.getsockname()[1] for s in reservations]
        self.peers = ",".join(f"127.0.0.1:{p}" for p in self.ports[:3])
        self.clients = ",".join(f"127.0.0.1:{p}" for p in self.ports[3:])
        for s in reservations: s.close()
        for i in range(1, 4): self.start(i)
    def start(self, node):
        log = open(self.root / f"node{node}.log", "ab", buffering=0)
        self.logs.setdefault(node, []).append(log)
        args = [self.binary, "node", "--id", str(node), "--dir", str(self.root / str(node)),
                "--peers", self.peers, "--clients", self.clients, "--snapshot-every", str(self.snapshot_every)]
        self.processes[node] = subprocess.Popen(args, stdout=log, stderr=log)
    def kill(self, node, sig=signal.SIGKILL):
        p = self.processes.pop(node, None)
        if p is not None:
            p.send_signal(sig); p.wait(timeout=8)
            if sig == signal.SIGTERM and p.returncode != 0:
                raise AssertionError(f"node {node} failed during graceful shutdown: {p.returncode}")
    def status(self, node):
        return request(self.ports[node+2], command(5))
    def leader(self, exclude=(), timeout=12):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            found = []
            for i, p in list(self.processes.items()):
                if i in exclude: continue
                if p.poll() is not None: raise AssertionError(f"node {i} exited: {p.returncode}")
                try:
                    status = self.status(i)
                    if status["role"] == "Leader": found.append((status["term"], i))
                except (OSError, EOFError): pass
            if found: return max(found)[1]
            time.sleep(.05)
        raise AssertionError("no leader")
    def call(self, op, key="", value="", client="test", seq=1, timeout=15):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            leader = self.leader(timeout=max(1, end-time.monotonic()))
            try:
                out = request(self.ports[leader+2], command(op,key,value,client,seq))
                if out["status"] not in ("RETRY","NOT_LEADER","BUSY"): return out
            except (OSError, EOFError): pass
            time.sleep(.05)
        raise AssertionError("request did not complete")
    def wait(self, predicate, timeout=12):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            try:
                if predicate(): return
            except (OSError, EOFError, KeyError): pass
            time.sleep(.05)
        raise AssertionError("condition did not converge")
    def close(self):
        for i in list(self.processes): self.kill(i, signal.SIGTERM)
        for logs in self.logs.values():
            for log in logs: log.close()
        if os.environ.get("RAFTKV_KEEP_TEST_DATA"):
            self.temp._finalizer.detach()
            print("TEST_DATA", self.root)
        else: self.temp.cleanup()

def check_linearizable(history):
    # 穷举小规模并发历史的串行顺序，同时尊重真实时间先后关系。
    before = [sum(1 << j for j, other in enumerate(history) if other["end"] < item["start"])
              for item in history]
    failed = set()
    def search(mask, value):
        if mask == (1 << len(history)) - 1: return True
        if (mask, value) in failed: return False
        for i, item in enumerate(history):
            if mask & (1 << i) or before[i] & ~mask: continue
            after = value
            if item["op"] == 1:
                if item["result"]["status"] != "OK": continue
                after = item["value"]
            elif item["op"] == 2:
                if item["result"]["status"] != ("NOT_FOUND" if value is None else "OK"): continue
                after = None
            else:
                if item["result"]["status"] != ("NOT_FOUND" if value is None else "OK"): continue
                if value is not None and item["result"].get("value") != value: continue
            if search(mask | (1 << i), after): return True
        failed.add((mask, value))
        return False
    assert search(0, None), history

def test_cluster():
    c = Cluster()
    try:
        leader = c.leader()
        print("PASS three-process election", leader, flush=True)
        assert c.call(1,"a","one","alice",1)["status"] == "OK"
        assert c.call(1,"a","two","bob",1)["status"] == "OK"
        assert c.call(1,"a","one","alice",1)["status"] == "OK"
        assert c.call(3,"a")["value"] == "two"
        assert c.call(1,"a","bad","alice",1)["status"] == "ID_REUSE"
        assert c.call(2,"a",client="alice",seq=2)["status"] == "OK"
        assert c.call(3,"a")["status"] == "NOT_FOUND"
        assert c.call(1,"a","one","alice",1)["status"] == "STALE_SEQUENCE"
        assert c.call(1,"空 格","值 with space","unicode",1)["status"] == "OK"
        assert c.call(3,"空 格")["value"] == "值 with space"
        print("PASS overwrite, tombstone, dedup, session sequence and UTF-8", flush=True)

        # 直接请求 follower，不能绕过 leader 返回本地旧值。
        leader = c.leader(); follower = next(i for i in c.processes if i != leader)
        assert request(c.ports[follower+2], command(3,"a"))["status"] == "NOT_LEADER"
        payload = command(1,"fragment","yes","fragmented",1)
        assert request(c.ports[leader+2], payload, fragmented=True)["status"] == "OK"
        with socket.create_connection(("127.0.0.1", c.ports[leader+2]), timeout=2) as s:
            s.sendall(struct.pack("!I", 16 * 1024 * 1024 + 1))
            s.settimeout(2)
            try: assert not s.recv(1)
            except ConnectionResetError: pass
        print("PASS follower redirect, fragmented TCP and oversized frame rejection", flush=True)

        def writer(i):
            for seq in range(1, 6):
                out = c.call(1, f"k{i}-{seq}", f"v{i}-{seq}", f"writer{i}", seq)
                assert out["status"] == "OK", out
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as pool:
            list(pool.map(writer, range(3)))
        rows = c.call(4,"k","l")["rows"]
        assert len(rows) == 15 and rows == sorted(rows)
        print("PASS concurrent clients and sorted half-open range read", flush=True)
        cli = subprocess.run([c.binary, "client", "--servers", c.clients,
                              "--client-id", "cli-test", "--seq", "1", "put", "cli-key", "cli value"],
                             capture_output=True, text=True, timeout=10)
        assert cli.returncode == 0 and json.loads(cli.stdout)["status"] == "OK", cli
        assert c.call(3, "cli-key")["value"] == "cli value"
        duplicate = subprocess.run(c.processes[leader].args, capture_output=True, text=True, timeout=3)
        assert duplicate.returncode != 0 and "locked" in duplicate.stderr
        print("PASS CLI redirect and live data-directory exclusivity", flush=True)

        history = []
        plans = [[(1, "A"), (3, ""), (2, ""), (3, "")],
                 [(1, "B"), (3, ""), (1, "C"), (3, "")]]
        def actor(actor_id):
            seq = 0
            for op, value in plans[actor_id]:
                if op in (1,2): seq += 1
                start = time.monotonic_ns()
                result = c.call(op, "linear-key", value, f"linear{actor_id}", seq)
                history.append({"op":op, "value":value, "result":result,
                                "start":start, "end":time.monotonic_ns()})
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            list(pool.map(actor, range(2)))
        check_linearizable(history)
        print("PASS bounded concurrent read/write/delete history is linearizable", flush=True)


        # 离线 follower 必须在 leader 已截断旧日志后通过快照追赶。
        leader = c.leader(); lagging = next(i for i in c.processes if i != leader)
        c.kill(lagging)
        for seq in range(1, 21):
            assert c.call(1, f"catch{seq:02}", "value", "catchup", seq)["status"] == "OK"
        snap = c.status(c.leader())["snapshot"]
        assert snap > 8
        c.start(lagging)
        c.wait(lambda: c.status(lagging)["snapshot"] >= snap)
        c.wait(lambda: c.status(lagging)["commit"] >= snap)
        print("PASS snapshot creation, log truncation and InstallSnapshot catch-up", flush=True)

        leader = c.leader()
        assert c.call(1,"durable","acknowledged","durable",1)["status"] == "OK"
        c.kill(leader)
        new = c.leader()
        assert new != leader
        assert c.call(3,"durable")["value"] == "acknowledged"
        c.start(leader)
        c.wait(lambda: c.status(leader)["role"] == "Follower")
        print("PASS SIGKILL leader, re-election and acknowledged-write preservation", flush=True)

        # SIGSTOP 隔离旧 leader，其他两节点继续服务；旧 leader 的读写均不能提交。
        old = c.leader()
        os.kill(c.processes[old].pid, signal.SIGSTOP)
        new = c.leader(exclude=(old,))
        assert c.call(1,"partition","majority","partition",1)["status"] == "OK"
        os.kill(c.processes[old].pid, signal.SIGCONT)
        c.wait(lambda: c.status(old)["role"] == "Follower")
        assert c.call(3,"partition")["value"] == "majority"
        print("PASS paused old leader rejoins higher-term majority", flush=True)

        # 只保留一个节点，连读屏障也不能提交。
        sole = c.leader()
        others = [i for i in c.processes if i != sole]
        for i in others: c.kill(i)
        write = request(c.ports[sole+2], command(1,"minority","bad","minority",1))
        read = request(c.ports[sole+2], command(3,"durable"))
        assert write["status"] == "RETRY" and read["status"] == "RETRY", (write,read)
        # 停掉带有未提交尾部的旧 leader，在另外两节点形成新多数派。
        c.kill(sole)
        for i in others: c.start(i)
        c.leader()
        assert c.call(1,"minority","majority","repair",1)["status"] == "OK"
        c.start(sole)
        committed = c.status(c.leader())["commit"]
        c.wait(lambda: c.status(sole)["commit"] >= committed)
        assert c.call(3,"minority")["value"] == "majority"
        assert c.call(3,"durable")["value"] == "acknowledged"
        print("PASS minority rejects read/write; conflicting uncommitted tail repaired", flush=True)

        for i in list(c.processes): c.kill(i)
        for i in range(1,4): c.start(i)
        c.leader()
        assert c.call(3,"durable")["value"] == "acknowledged"
        assert c.call(3,"a")["status"] == "NOT_FOUND"
        assert c.call(1,"a","one","alice",1)["status"] == "STALE_SEQUENCE"
        assert c.call(1,"a","two","bob",1)["status"] == "OK"
        assert c.call(3,"a")["status"] == "NOT_FOUND"
        print("PASS all-node abrupt restart, deletion and dedup recovery", flush=True)
    except BaseException:
        print("FAILED DATA", c.root, flush=True)
        for file in c.root.glob("*.log"):
            print(file.name, file.read_text(errors="replace")[-4000:], flush=True)
        raise
    finally: c.close()

if __name__ == "__main__": test_cluster()
