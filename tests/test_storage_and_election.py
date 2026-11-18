import asyncio
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock

import cluster_pb2 as pb
from node import Node
from settings import Settings
from storage import Storage

ROOT = Path(__file__).resolve().parents[1]


class StorageTests(unittest.TestCase):
    def test_persistencia_e_visibilidade_do_commit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "node.sqlite3"
            store = Storage(path)
            store.term = 7
            store.voted_for = 2
            store.log = [pb.Entry(index=1, term=7, key="x", value="old"),
                         pb.Entry(index=2, term=7, key="x", value="new")]
            store.commit_index = 1
            store.salvar()
            store.fechar()
            store = Storage(path)
            self.assertEqual((store.term, store.voted_for, store.commit_index), (7, 2, 1))
            self.assertEqual(len(store.log), 2)
            self.assertEqual(store.consultar("x"), "old")
            store.commit_index = 2
            store.salvar()
            self.assertEqual(store.consultar("x"), "new")
            store.fechar()


class ElectionTests(unittest.IsolatedAsyncioTestCase):
    # Estes nomes sao pontos de entrada obrigatorios do unittest.
    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.path = Path(self.directory.name) / "node.sqlite3"
        self.store = Storage(self.path)
        self.node = Node(1, Settings.carregar(str(ROOT / "config.json")), self.store)

    async def asyncTearDown(self):
        await asyncio.gather(*(channel.close() for channel in self.node.channels.values()))
        self.store.fechar()
        self.directory.cleanup()

    async def test_voto_unico_por_termo_sobrevive_ao_reinicio(self):
        granted = await self.node.solicitar_voto(pb.VoteRequest(term=1, candidate_id=2), None)
        denied = await self.node.solicitar_voto(pb.VoteRequest(term=1, candidate_id=3), None)
        self.assertTrue(granted.granted)
        self.assertFalse(denied.granted)
        reopened = Storage(self.path)
        self.assertEqual(reopened.voted_for, 2)
        reopened.fechar()
        granted_next = await self.node.solicitar_voto(pb.VoteRequest(term=2, candidate_id=3), None)
        self.assertTrue(granted_next.granted)

    async def test_atualizacao_do_log_compara_termo_depois_indice(self):
        self.store.term = 5
        self.store.log = [pb.Entry(index=1, term=4, noop=True)]
        self.store.salvar()
        for last_term, last_index in [(3, 100), (4, 0)]:
            response = await self.node.solicitar_voto(
                pb.VoteRequest(term=5, candidate_id=2,
                               last_term=last_term, last_index=last_index), None
            )
            self.assertFalse(response.granted)
        response = await self.node.solicitar_voto(
            pb.VoteRequest(term=5, candidate_id=3, last_term=4, last_index=1), None
        )
        self.assertTrue(response.granted)

    async def test_termo_maior_remove_lideranca_mesmo_em_busca(self):
        self.node.role = "lider"
        response = await self.node.atender_busca(
            pb.FetchRequest(term=10, follower_id=2), None
        )
        self.assertEqual(response.term, 10)
        self.assertFalse(response.success)
        self.assertEqual(self.node.role, "seguidor")
        self.assertIsNone(self.store.voted_for)

    async def test_commit_exige_tres_confirmacoes_e_termo_atual(self):
        self.node.role = "lider"
        self.store.term = 2
        self.store.log = [pb.Entry(index=1, term=1, noop=True),
                          pb.Entry(index=2, term=2, noop=True)]
        async with self.node.changed:
            self.node.matches = {2: 2}
            self.node.avancar_commit()
            self.assertEqual(self.store.commit_index, 0)
            self.node.matches = {2: 1, 3: 1}
            self.node.avancar_commit()
            self.assertEqual(self.store.commit_index, 0)
            self.node.matches = {2: 2, 3: 2}
            self.node.avancar_commit()
            self.assertEqual(self.store.commit_index, 2)

    async def test_sem_quorum_rejeita_sem_acrescentar_entrada(self):
        self.node.role = "lider"
        response = await self.node.gravar(pb.WriteRequest(key="x", value="hidden"), None)
        self.assertEqual(response.error, "NO_QUORUM")
        self.assertFalse(response.committed)
        self.assertEqual(len(self.store.log), 0)

    async def test_busca_confirma_apenas_entradas_ja_armazenadas(self):
        self.node.role = "lider"
        self.node.leader_id = 1
        self.store.term = 1
        self.store.log = [pb.Entry(index=1, term=1, key="x", value="value")]
        self.store.salvar()
        for peer in (2, 3):
            fetched = await self.node.atender_busca(
                pb.FetchRequest(term=1, follower_id=peer), None
            )
            self.assertEqual(len(fetched.entries), 1)
            self.assertEqual(fetched.commit_index, 0)
        self.assertEqual(self.store.commit_index, 0)
        first_ack = await self.node.atender_busca(
            pb.FetchRequest(term=1, follower_id=2, last_index=1, last_term=1), None
        )
        self.assertEqual(first_ack.commit_index, 0)
        second_ack = await self.node.atender_busca(
            pb.FetchRequest(term=1, follower_id=3, last_index=1, last_term=1), None
        )
        self.assertEqual(second_ack.commit_index, 1)
        self.assertEqual(self.store.consultar("x"), "value")
        self.node.last_fetch[2] = time.monotonic() - self.node.cfg.heartbeat_timeout - 1
        self.assertNotIn(2, self.node.obter_replicas_ativas())
        self.assertIn(3, self.node.obter_replicas_ativas())

    async def test_termo_maior_na_resposta_de_voto_cancela_eleicao(self):
        for peer in self.node.peers:
            self.node.peers[peer] = AsyncMock()
            self.node.peers[peer].RequestVote.return_value = pb.VoteResponse(
                term=9, granted=False
            )
        await self.node.iniciar_eleicao()
        self.assertEqual(self.store.term, 9)
        self.assertEqual(self.node.role, "seguidor")

    async def test_termo_maior_na_resposta_de_busca_e_persistido(self):
        self.node.peers[2] = AsyncMock()
        self.node.peers[2].Fetch.return_value = pb.FetchResponse(term=8, leader_id=2)
        await self.node.buscar_do_lider(2)
        self.assertEqual(self.store.term, 8)
        disk = Storage(self.path)
        self.assertEqual(disk.term, 8)
        disk.fechar()

    async def test_busca_recua_e_remove_cauda_excedente(self):
        self.store.term = 2
        self.store.log = [pb.Entry(index=1, term=1, key="extra", value="hidden")]
        self.store.salvar()
        self.node.peers[2] = AsyncMock()
        self.node.peers[2].Fetch.side_effect = [
            pb.FetchResponse(term=2, leader_id=2, retry_index=0),
            pb.FetchResponse(term=2, leader_id=2, success=True),
        ]
        await self.node.buscar_do_lider(2)
        self.assertEqual(self.store.log, [])
        self.assertEqual(self.node.truncations, 1)
        self.assertEqual(self.node.role, "seguidor")
        self.assertIsNone(self.store.consultar("extra"))

    async def test_busca_divide_logs_grandes_em_lotes(self):
        self.node.role = "lider"
        self.node.leader_id = 1
        self.store.term = 1
        self.store.log = [pb.Entry(index=i, term=1, key=str(i), value="x" * 65536)
                          for i in range(1, 66)]
        response = await self.node.atender_busca(pb.FetchRequest(term=1, follower_id=2), None)
        self.assertEqual(len(response.entries), 32)
        self.assertLess(response.ByteSize(), 4 * 1024 * 1024)


if __name__ == "__main__":
    unittest.main()
