import concurrent.futures
import json
import socket
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

import grpc

import cluster_pb2 as pb
import cluster_pb2_grpc as rpc
from client import Client
from settings import Settings

ROOT = Path(__file__).resolve().parents[1]


class ClusterTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory(prefix="grpc-pull-tests-")
        self.path = Path(self.directory.name)
        sockets = []
        ports = []
        for _ in range(4):
            sock = socket.socket()
            sock.bind(("127.0.0.1", 0))
            sockets.append(sock)
            ports.append(sock.getsockname()[1])
        raw = json.loads((ROOT / "config.json").read_text(encoding="utf-8"))
        raw["nodes"] = {str(i): f"127.0.0.1:{port}" for i, port in enumerate(ports, 1)}
        self.config = self.path / "config.json"
        self.config.write_text(json.dumps(raw), encoding="utf-8")
        self.cfg = Settings.load(str(self.config))
        self.processes = {}
        self.files = {}
        self.launches = {i: 0 for i in range(1, 5)}
        for sock in sockets:
            sock.close()
        self.client = Client(self.cfg)
        self.addCleanup(self.cleanup_cluster)
        for node in range(1, 5):
            self.start(node)
        self.leader = self.wait_leader()
        self.wait(lambda: self.status(self.leader).commit_index >= 1
                  and sum(p.active for p in self.status(self.leader).peers) == 3)

    def cleanup_cluster(self):
        for process in self.processes.values():
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)
        for file in self.files.values():
            file.close()
        self.client.close()
        if sys.exc_info()[0] is not None or not self._outcome.success:
            for path in self.path.glob("*.log"):
                print(f"\n--- {path.name} ---")
                print("\n".join(path.read_text(encoding="utf-8", errors="replace").splitlines()[-35:]))
        self.directory.cleanup()

    def start(self, node):
        self.launches[node] += 1
        name = f"node-{node}-{self.launches[node]}.log"
        output = (self.path / name).open("w", encoding="utf-8")
        self.files[name] = output
        self.processes[node] = subprocess.Popen(
            [sys.executable, "-u", str(ROOT / "node.py"), "--id", str(node),
             "--config", str(self.config), "--data-dir", str(self.path / "data"),
             "--enable-test-controls"],
            cwd=ROOT, stdout=output, stderr=subprocess.STDOUT,
        )
        # Fail fast if bind/startup fails instead of masking it as election trouble.
        self.wait(lambda: self.status(node) is not None, timeout=8)

    def stop(self, node):
        process = self.processes[node]
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)

    def status(self, node):
        try:
            return self.client.stubs[node].Status(pb.Empty(), timeout=0.25)
        except grpc.RpcError:
            return None

    def wait(self, predicate, timeout=12):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            result = predicate()
            if result:
                return result
            time.sleep(0.05)
        self.fail("Condicao nao atingida dentro do prazo.")

    def wait_leader(self, exclude=()):
        def find():
            statuses = [self.status(n) for n in range(1, 5) if n not in exclude]
            leaders = [s for s in statuses if s is not None and s.role == "lider"]
            return leaders[0].node_id if len(leaders) == 1 else None
        return self.wait(find)

    def read(self, node, key):
        return self.client.stubs[node].Read(pb.ReadRequest(key=key), timeout=1)

    def wait_value(self, key, value, nodes=range(1, 5)):
        self.wait(lambda: all(self.read(n, key).found and self.read(n, key).value == value
                              for n in nodes))

    def put(self, key, value, node=None):
        response = self.client.write(node or self.leader, key, value)
        self.assertTrue(response.committed, response)
        self.assertFalse(response.error)
        return response

    def test_01_normal_write_redirection_distributed_read(self):
        follower = next(n for n in range(1, 5) if n != self.leader)
        self.wait(lambda: self.status(follower).leader_id == self.leader)
        redirect = self.client.stubs[follower].Write(
            pb.WriteRequest(key="produto", value="42"), timeout=1
        )
        self.assertEqual(redirect.error, "NOT_LEADER")
        self.assertEqual(redirect.leader_id, self.leader)
        response = self.put("produto", "42", follower)
        self.assertGreaterEqual(self.status(self.leader).commit_index, response.index)
        self.wait_value("produto", "42")

    def test_02_leader_crash_new_term_and_durable_commit(self):
        self.put("duravel", "antes")
        self.wait_value("duravel", "antes")
        old = self.leader
        term = self.status(old).term
        self.stop(old)
        self.leader = self.wait_leader(exclude=(old,))
        self.assertGreater(self.status(self.leader).term, term)
        self.wait(lambda: sum(p.active for p in self.status(self.leader).peers) >= 2)
        self.put("apos", "eleicao")
        self.start(old)
        self.wait_value("apos", "eleicao")
        self.wait_value("duravel", "antes")
        self.assertEqual(self.status(old).role, "seguidor")

    def test_03_quorum_loss_rejects_write_without_append(self):
        victims = [n for n in range(1, 5) if n != self.leader][:2]
        for node in victims:
            self.stop(node)
        self.wait(lambda: sum(p.active for p in self.status(self.leader).peers) == 1)
        before = self.status(self.leader)
        response = self.client.stubs[self.leader].Write(
            pb.WriteRequest(key="nao", value="quorum"), timeout=1
        )
        self.assertEqual(response.error, "NO_QUORUM")
        after = self.status(self.leader)
        self.assertEqual(after.last_index, before.last_index)
        self.assertEqual(after.commit_index, before.commit_index)
        self.assertFalse(self.read(self.leader, "nao").found)

    def test_04a_offline_follower_catches_up_via_fetch(self):
        follower = next(n for n in range(1, 5) if n != self.leader)
        self.stop(follower)
        for i in range(3):
            self.put(f"missed-{i}", str(i))
        self.start(follower)
        for i in range(3):
            self.wait_value(f"missed-{i}", str(i))

    def test_04b_divergent_uncommitted_suffix_is_truncated(self):
        old = self.leader
        before = self.status(old)
        with self.assertRaises(grpc.RpcError):
            self.client.write(old, "fantasma", "nunca confirmado", crash=True)
        self.processes[old].wait(timeout=8)
        self.assertEqual(self.processes[old].returncode, 86)
        # Read the killed node's disk only to prove the intended crash point.
        from storage import Storage
        disk = Storage(self.path / "data" / f"node-{old}.sqlite3")
        self.assertEqual(disk.log[-1].key, "fantasma")
        self.assertEqual(disk.log[-1].index, before.last_index + 1)
        self.assertLess(disk.commit_index, disk.log[-1].index)
        disk.close()
        self.leader = self.wait_leader(exclude=(old,))
        self.assertGreater(self.status(self.leader).term, before.term)
        self.wait(lambda: sum(p.active for p in self.status(self.leader).peers) >= 2)
        self.put("correto", "novo termo")
        self.start(old)
        self.wait_value("correto", "novo termo")
        self.wait(lambda: self.status(old).truncations >= 1)
        self.assertEqual(self.status(old).role, "seguidor")
        for node in range(1, 5):
            self.assertFalse(self.read(node, "fantasma").found)
        disk = Storage(self.path / "data" / f"node-{old}.sqlite3")
        self.assertNotIn("fantasma", [entry.key for entry in disk.log])
        disk.close()

    def test_05_fetched_uncommitted_data_is_invisible(self):
        stub = self.client.stubs[self.leader]
        stub.Control(pb.ControlRequest(pause_commit=True), timeout=1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.client.write, self.leader, "oculto", "so depois")
            initial = self.status(self.leader).last_index
            self.wait(lambda: all(self.status(n).last_index >= initial
                                  for n in range(1, 5))
                      and self.status(self.leader).last_index >
                      self.status(self.leader).commit_index)
            # Inspect followers' durable logs, not just status, to prove receipt.
            from storage import Storage
            for node in range(1, 5):
                def persisted():
                    disk = Storage(self.path / "data" / f"node-{node}.sqlite3")
                    try:
                        return any(e.key == "oculto" for e in disk.log)
                    finally:
                        disk.close()
                self.wait(persisted)
                self.assertFalse(self.read(node, "oculto").found)
            self.assertFalse(future.done(), "Cliente confirmou antes do commit.")
            stub.Control(pb.ControlRequest(pause_commit=False), timeout=1)
            self.assertTrue(future.result(timeout=6).committed)
        self.wait_value("oculto", "so depois")

    def test_06_all_nodes_restart_preserves_data_and_term(self):
        self.put("persistente", "disco")
        self.wait_value("persistente", "disco")
        terms = {n: self.status(n).term for n in range(1, 5)}
        for node in range(1, 5):
            self.stop(node)
        for node in range(1, 5):
            self.start(node)
            self.assertEqual(self.read(node, "persistente").value, "disco")
            self.assertGreaterEqual(self.status(node).term, terms[node])
        self.leader = self.wait_leader()
        self.wait(lambda: sum(p.active for p in self.status(self.leader).peers) >= 2)
        self.put("reinicio", "ok")
        self.wait_value("reinicio", "ok")


if __name__ == "__main__":
    unittest.main()
