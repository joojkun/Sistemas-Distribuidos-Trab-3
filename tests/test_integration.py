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
    # O unittest chama setUp antes de cada cenario; usa portas e dados isolados.
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
        self.cfg = Settings.carregar(str(self.config))
        self.processes = {}
        self.files = {}
        self.launches = {i: 0 for i in range(1, 5)}
        for sock in sockets:
            sock.close()
        self.client = Client(self.cfg)
        self.addCleanup(self.encerrar_cluster)
        for node in range(1, 5):
            self.iniciar_no(node)
        self.leader = self.aguardar_lider()
        self.aguardar(lambda: self.consultar_estado(self.leader).commit_index >= 1
                  and sum(p.active for p in self.consultar_estado(self.leader).peers) == 3)

    def encerrar_cluster(self):
        for process in self.processes.values():
            if process.poll() is None:
                process.kill()
            process.wait(timeout=10)
        for file in self.files.values():
            file.close()
        self.client.fechar()
        if sys.exc_info()[0] is not None or not self._outcome.success:
            for path in self.path.glob("*.log"):
                print(f"\n--- {path.name} ---")
                print("\n".join(path.read_text(encoding="utf-8", errors="replace").splitlines()[-35:]))
        self.directory.cleanup()

    def iniciar_no(self, node):
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
        # Detecta falha ao iniciar antes de confundi-la com problema de eleicao.
        self.aguardar(lambda: self.consultar_estado(node) is not None, timeout=8)

    def parar_no(self, node):
        process = self.processes[node]
        if process.poll() is None:
            process.kill()
        process.wait(timeout=10)

    def consultar_estado(self, node):
        try:
            return self.client.stubs[node].Status(pb.Empty(), timeout=0.25)
        except grpc.RpcError:
            return None

    def aguardar(self, predicate, timeout=12):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            result = predicate()
            if result:
                return result
            time.sleep(0.05)
        self.fail("Condicao nao atingida dentro do prazo.")

    def aguardar_lider(self, exclude=()):
        def encontrar_lider():
            statuses = [self.consultar_estado(n) for n in range(1, 5) if n not in exclude]
            leaders = [s for s in statuses if s is not None and s.role == "lider"]
            return leaders[0].node_id if len(leaders) == 1 else None
        return self.aguardar(encontrar_lider)

    def consultar(self, node, key):
        return self.client.stubs[node].Read(pb.ReadRequest(key=key), timeout=1)

    def aguardar_valor(self, key, value, nodes=range(1, 5)):
        self.aguardar(lambda: all(self.consultar(n, key).found and self.consultar(n, key).value == value
                              for n in nodes))

    def gravar(self, key, value, node=None):
        response = self.client.gravar(node or self.leader, key, value)
        self.assertTrue(response.committed, response)
        self.assertFalse(response.error)
        return response

    def test_01_gravacao_redirecionamento_e_leitura_distribuida(self):
        follower = next(n for n in range(1, 5) if n != self.leader)
        self.aguardar(lambda: self.consultar_estado(follower).leader_id == self.leader)
        redirect = self.client.stubs[follower].Write(
            pb.WriteRequest(key="produto", value="42"), timeout=1
        )
        self.assertEqual(redirect.error, "NOT_LEADER")
        self.assertEqual(redirect.leader_id, self.leader)
        response = self.gravar("produto", "42", follower)
        self.assertGreaterEqual(self.consultar_estado(self.leader).commit_index, response.index)
        self.aguardar_valor("produto", "42")

    def test_02_queda_do_lider_novo_termo_e_commit_persistente(self):
        self.gravar("duravel", "antes")
        self.aguardar_valor("duravel", "antes")
        old = self.leader
        term = self.consultar_estado(old).term
        self.parar_no(old)
        self.leader = self.aguardar_lider(exclude=(old,))
        self.assertGreater(self.consultar_estado(self.leader).term, term)
        self.aguardar(lambda: sum(p.active for p in self.consultar_estado(self.leader).peers) >= 2)
        self.gravar("apos", "eleicao")
        self.iniciar_no(old)
        self.aguardar_valor("apos", "eleicao")
        self.aguardar_valor("duravel", "antes")
        self.assertEqual(self.consultar_estado(old).role, "seguidor")

    def test_03_perda_de_quorum_rejeita_sem_acrescentar_entrada(self):
        victims = [n for n in range(1, 5) if n != self.leader][:2]
        for node in victims:
            self.parar_no(node)
        self.aguardar(lambda: sum(p.active for p in self.consultar_estado(self.leader).peers) == 1)
        before = self.consultar_estado(self.leader)
        response = self.client.stubs[self.leader].Write(
            pb.WriteRequest(key="nao", value="quorum"), timeout=1
        )
        self.assertEqual(response.error, "NO_QUORUM")
        after = self.consultar_estado(self.leader)
        self.assertEqual(after.last_index, before.last_index)
        self.assertEqual(after.commit_index, before.commit_index)
        self.assertFalse(self.consultar(self.leader, "nao").found)

    def test_04a_seguidor_desligado_recupera_entradas_por_busca(self):
        follower = next(n for n in range(1, 5) if n != self.leader)
        self.parar_no(follower)
        for i in range(3):
            self.gravar(f"missed-{i}", str(i))
        self.iniciar_no(follower)
        for i in range(3):
            self.aguardar_valor(f"missed-{i}", str(i))

    def test_04b_cauda_divergente_nao_comitada_e_truncada(self):
        old = self.leader
        before = self.consultar_estado(old)
        with self.assertRaises(grpc.RpcError):
            self.client.gravar(old, "fantasma", "nunca confirmado", crash=True)
        self.processes[old].wait(timeout=8)
        self.assertEqual(self.processes[old].returncode, 86)
        # Inspeciona o disco do no encerrado para comprovar o ponto exato da queda.
        from storage import Storage
        disk = Storage(self.path / "data" / f"node-{old}.sqlite3")
        self.assertEqual(disk.log[-1].key, "fantasma")
        self.assertEqual(disk.log[-1].index, before.last_index + 1)
        self.assertLess(disk.commit_index, disk.log[-1].index)
        disk.fechar()
        self.leader = self.aguardar_lider(exclude=(old,))
        self.assertGreater(self.consultar_estado(self.leader).term, before.term)
        self.aguardar(lambda: sum(p.active for p in self.consultar_estado(self.leader).peers) >= 2)
        self.gravar("correto", "novo termo")
        self.iniciar_no(old)
        self.aguardar_valor("correto", "novo termo")
        self.aguardar(lambda: self.consultar_estado(old).truncations >= 1)
        self.assertEqual(self.consultar_estado(old).role, "seguidor")
        for node in range(1, 5):
            self.assertFalse(self.consultar(node, "fantasma").found)
        disk = Storage(self.path / "data" / f"node-{old}.sqlite3")
        self.assertNotIn("fantasma", [entry.key for entry in disk.log])
        disk.fechar()

    def test_05_dado_recebido_sem_commit_fica_invisivel(self):
        stub = self.client.stubs[self.leader]
        stub.Control(pb.ControlRequest(pause_commit=True), timeout=1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            future = pool.submit(self.client.gravar, self.leader, "oculto", "so depois")
            initial = self.consultar_estado(self.leader).last_index
            self.aguardar(lambda: all(self.consultar_estado(n).last_index >= initial
                                  for n in range(1, 5))
                      and self.consultar_estado(self.leader).last_index >
                      self.consultar_estado(self.leader).commit_index)
            # Confere o log em disco para provar que as replicas receberam a entrada.
            from storage import Storage
            for node in range(1, 5):
                def entrada_persistida():
                    disk = Storage(self.path / "data" / f"node-{node}.sqlite3")
                    try:
                        return any(e.key == "oculto" for e in disk.log)
                    finally:
                        disk.fechar()
                self.aguardar(entrada_persistida)
                self.assertFalse(self.consultar(node, "oculto").found)
            self.assertFalse(future.done(), "Cliente confirmou antes do commit.")
            stub.Control(pb.ControlRequest(pause_commit=False), timeout=1)
            self.assertTrue(future.result(timeout=6).committed)
        self.aguardar_valor("oculto", "so depois")

    def test_06_reinicio_de_todos_os_nos_preserva_dados_e_termo(self):
        self.gravar("persistente", "disco")
        self.aguardar_valor("persistente", "disco")
        terms = {n: self.consultar_estado(n).term for n in range(1, 5)}
        for node in range(1, 5):
            self.parar_no(node)
        for node in range(1, 5):
            self.iniciar_no(node)
            self.assertEqual(self.consultar(node, "persistente").value, "disco")
            self.assertGreaterEqual(self.consultar_estado(node).term, terms[node])
        self.leader = self.aguardar_lider()
        self.aguardar(lambda: sum(p.active for p in self.consultar_estado(self.leader).peers) >= 2)
        self.gravar("reinicio", "ok")
        self.aguardar_valor("reinicio", "ok")


if __name__ == "__main__":
    unittest.main()
