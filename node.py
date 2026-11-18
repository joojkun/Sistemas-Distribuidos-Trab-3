import argparse
import asyncio
import logging
import os
import random
import time
from pathlib import Path

import grpc

import cluster_pb2 as pb
import cluster_pb2_grpc as rpc
from settings import Settings
from storage import Storage

LOG = logging.getLogger("cluster")
QUORUM = 3


class Node(rpc.ClusterServicer):
    def __init__(self, node_id: int, settings: Settings, storage: Storage,
                 test_controls: bool = False):
        self.id = node_id
        self.cfg = settings
        self.store = storage
        self.test_controls = test_controls
        self.role = "seguidor"
        self.leader_id = 0
        # A condicao protege o estado e permite esperar commit sem bloquear os Fetches.
        self.changed = asyncio.Condition()
        self.last_fetch: dict[int, float] = {}
        self.matches: dict[int, int] = {}
        self.fetch_paused = False
        self.commit_paused = False
        self.truncations = 0
        self.reported_active: set[int] = set()
        self.channels = {
            peer: grpc.aio.insecure_channel(address)
            for peer, address in settings.nodes.items() if peer != node_id
        }
        self.peers = {peer: rpc.ClusterStub(channel)
                      for peer, channel in self.channels.items()}
        self.reiniciar_prazo_eleicao()

    def reiniciar_prazo_eleicao(self) -> None:
        # Prazos diferentes reduzem a chance de candidaturas simultaneas.
        self.deadline = time.monotonic() + random.uniform(
            self.cfg.election_min, self.cfg.election_max
        )

    def registrar_evento(self, event: str) -> None:
        LOG.info("no=%s papel=%s termo=%s commit=%s log=%s %s",
                 self.id, self.role, self.store.term, self.store.commit_index,
                 len(self.store.log), event)

    def observar_termo(self, term: int) -> None:
        """Abandona a lideranca antiga ao conhecer uma rodada de eleicao maior."""
        if term > self.store.term:
            self.store.term = term
            self.store.voted_for = None
            self.store.salvar()
            self.role = "seguidor"
            self.leader_id = 0
            self.last_fetch.clear()
            self.matches.clear()
            self.reiniciar_prazo_eleicao()
            self.changed.notify_all()
            self.registrar_evento("termo maior recebido; transicao para seguidor")

    def obter_replicas_ativas(self) -> set[int]:
        now = time.monotonic()
        return {peer for peer, instant in self.last_fetch.items()
                if now - instant <= self.cfg.heartbeat_timeout}

    def avancar_commit(self) -> None:
        if self.role != "lider" or self.commit_paused:
            return
        # O terceiro maior indice e o ponto armazenado por pelo menos tres nos.
        confirmed = sorted([len(self.store.log)] + [
            self.matches.get(peer, 0) for peer in self.peers
        ], reverse=True)[QUORUM - 1]
        # Entradas de termos antigos sao confirmadas atraves de uma do termo atual.
        if (confirmed > self.store.commit_index
                and self.store.obter_termo_no_indice(confirmed) == self.store.term):
            self.store.commit_index = confirmed
            self.store.salvar()
            self.changed.notify_all()
            self.registrar_evento("commit confirmado por quorum")

    async def solicitar_voto(self, request, context):
        if request.candidate_id not in self.peers:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Candidato invalido.")
        async with self.changed:
            self.observar_termo(request.term)
            # Compara primeiro o termo da ultima entrada; o indice desempata.
            up_to_date = (request.last_term, request.last_index) >= (
                self.store.obter_termo_no_indice(len(self.store.log)), len(self.store.log)
            )
            granted = (
                request.term == self.store.term and up_to_date
                and self.store.voted_for in (None, request.candidate_id)
            )
            if granted:
                self.store.voted_for = request.candidate_id
                # O voto deve sobreviver a reinicios antes de ser informado ao candidato.
                self.store.salvar()
                self.reiniciar_prazo_eleicao()
                self.registrar_evento(f"voto concedido a {request.candidate_id}")
            return pb.VoteResponse(term=self.store.term, granted=granted)

    async def atender_busca(self, request, context):
        """Responde ao Fetch iniciado pela replica; nao envia dados espontaneamente."""
        if request.follower_id not in self.peers or request.last_index < 0:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Fetch invalido.")
        async with self.changed:
            self.observar_termo(request.term)
            response = pb.FetchResponse(
                term=self.store.term,
                leader_id=self.id if self.role == "lider" else self.leader_id,
            )
            if self.role != "lider" or request.term != self.store.term:
                return response
            self.last_fetch[request.follower_id] = time.monotonic()
            index = request.last_index
            if (index > len(self.store.log)
                    or self.store.obter_termo_no_indice(index) != request.last_term):
                response.retry_index = min(max(0, index - 1), len(self.store.log))
                return response
            # Confirma apenas o que a replica informou ja ter, nao o lote a enviar.
            self.matches[request.follower_id] = index
            self.avancar_commit()
            response.success = True
            response.entries.extend(self.store.log[index:index + 32])
            response.commit_index = self.store.commit_index
            return response

    async def gravar(self, request, context):
        if (not request.key or len(request.key.encode("utf-8")) > 256
                or len(request.value.encode("utf-8")) > 65536):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Chave/valor invalidos.")
        if request.crash_after_append and not self.test_controls:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Controles de teste desativados.")
        async with self.changed:
            def montar_resposta(error: str = "", committed: bool = False, index: int = 0):
                return pb.WriteResponse(
                    term=self.store.term, leader_id=self.leader_id,
                    committed=committed, index=index, error=error,
                )
            if self.role != "lider":
                return montar_resposta("NOT_LEADER")
            if len(self.obter_replicas_ativas()) + 1 < QUORUM:
                self.registrar_evento("escrita recusada: NO_QUORUM")
                return montar_resposta("NO_QUORUM")
            term = self.store.term
            index = len(self.store.log) + 1
            entry = pb.Entry(index=index, term=term, key=request.key, value=request.value)
            self.store.log.append(entry)
            self.store.salvar()
            self.registrar_evento(f"append local indice={index} chave={request.key}")
            if request.crash_after_append:
                self.registrar_evento("TESTE: crash apos persistencia e antes da replicacao")
                # Sem um await, nenhum Fetch pode copiar esta entrada antes da queda.
                os._exit(86)
            deadline = time.monotonic() + self.cfg.write_timeout
            while True:
                if self.role != "lider" or self.store.term != term:
                    return montar_resposta("LEADERSHIP_CHANGED; resultado indeterminado", index=index)
                if self.store.commit_index >= index:
                    return montar_resposta(committed=True, index=index)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.registrar_evento(f"escrita sem confirmacao indice={index}")
                    return montar_resposta("COMMIT_TIMEOUT; resultado indeterminado", index=index)
                try:
                    # Libera a condicao enquanto aguarda, para atender confirmacoes.
                    await asyncio.wait_for(self.changed.wait(), remaining)
                except asyncio.TimeoutError:
                    return montar_resposta("COMMIT_TIMEOUT; resultado indeterminado", index=index)

    async def consultar(self, request, context):
        async with self.changed:
            # A consulta ignora operacoes pendentes e nao procura o lider.
            value = self.store.consultar(request.key)
            return pb.ReadResponse(term=self.store.term, found=value is not None,
                                   value=value if value is not None else "",
                                   commit_index=self.store.commit_index)

    def montar_estado(self):
        active = self.obter_replicas_ativas()
        return pb.StatusResponse(
            node_id=self.id, role=self.role, term=self.store.term,
            leader_id=self.leader_id, commit_index=self.store.commit_index,
            last_index=len(self.store.log), fetch_paused=self.fetch_paused,
            commit_paused=self.commit_paused, truncations=self.truncations,
            peers=[pb.PeerStatus(node_id=peer, active=peer in active,
                                 match_index=self.matches.get(peer, 0))
                   for peer in self.peers],
        )

    async def consultar_estado(self, request, context):
        return self.montar_estado()

    async def controlar_testes(self, request, context):
        if not self.test_controls:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Controles de teste desativados.")
        async with self.changed:
            if request.HasField("pause_fetch"):
                self.fetch_paused = request.pause_fetch
            if request.HasField("pause_commit"):
                self.commit_paused = request.pause_commit
                self.avancar_commit()
            self.registrar_evento(f"TESTE: fetch_paused={self.fetch_paused} commit_paused={self.commit_paused}")
            return self.montar_estado()

    # O codigo gerado exige estes nomes do contrato gRPC. Os aliases mantem
    # o protocolo original, mas a implementacao e lida pelos nomes em portugues.
    RequestVote = solicitar_voto
    Fetch = atender_busca
    Write = gravar
    Read = consultar
    Status = consultar_estado
    Control = controlar_testes

    async def iniciar_eleicao(self):
        async with self.changed:
            self.role = "candidato"
            self.leader_id = 0
            self.store.term += 1
            self.store.voted_for = self.id
            self.store.salvar()
            self.reiniciar_prazo_eleicao()
            term = self.store.term
            request = pb.VoteRequest(
                term=term, candidate_id=self.id, last_index=len(self.store.log),
                last_term=self.store.obter_termo_no_indice(len(self.store.log)),
            )
            self.registrar_evento("iniciando eleicao")

        async def pedir_voto_ao_no(peer, stub):
            try:
                return await stub.RequestVote(request, timeout=self.cfg.rpc_timeout)
            except grpc.aio.AioRpcError as error:
                LOG.debug("RequestVote no=%s destino=%s erro=%s", self.id, peer, error.code())
                return None

        responses = await asyncio.gather(*(pedir_voto_ao_no(p, s) for p, s in self.peers.items()))
        async with self.changed:
            for response in responses:
                if response is not None:
                    self.observar_termo(response.term)
            if self.store.term != term or self.role != "candidato":
                return
            if 1 + sum(r is not None and r.term == term and r.granted for r in responses) >= QUORUM:
                self.role = "lider"
                self.leader_id = self.id
                self.last_fetch.clear()
                self.matches.clear()
                self.reported_active.clear()
                # A noop nao muda chaves; permite confirmar o prefixo de termos antigos.
                self.store.log.append(pb.Entry(
                    index=len(self.store.log) + 1, term=term, noop=True
                ))
                self.store.salvar()
                self.registrar_evento("eleito com pelo menos 3 votos; noop do termo acrescentado")

    async def buscar_do_lider(self, leader: int):
        async with self.changed:
            term = self.store.term
            cursor = len(self.store.log)
        # Recua so o cursor ate encontrar um prefixo comum; nao apaga o log ainda.
        while True:
            async with self.changed:
                if self.role == "lider" or self.store.term != term or self.fetch_paused:
                    return
                request = pb.FetchRequest(
                    term=term, follower_id=self.id, last_index=cursor,
                    last_term=self.store.obter_termo_no_indice(cursor),
                )
            try:
                response = await self.peers[leader].Fetch(
                    request, timeout=self.cfg.rpc_timeout
                )
            except grpc.aio.AioRpcError as error:
                LOG.debug("Fetch no=%s destino=%s erro=%s", self.id, leader, error.code())
                return
            async with self.changed:
                self.observar_termo(response.term)
                if self.store.term != term or self.role == "lider" or response.term != term:
                    return
                if response.leader_id != leader:
                    if response.leader_id in self.peers:
                        self.leader_id = response.leader_id
                    return
                if not response.success:
                    if cursor == 0 or not 0 <= response.retry_index < cursor:
                        return
                    cursor = response.retry_index
                    continue
                self.role = "seguidor"
                self.leader_id = leader
                self.reiniciar_prazo_eleicao()
                incoming = list(response.entries)
                if any(e.index != cursor + i + 1 for i, e in enumerate(incoming)):
                    raise RuntimeError("Resposta Fetch com indices invalidos.")
                replacement = self.store.log[:cursor] + incoming
                # Somente a cauda nao comitada pode ser corrigida pelo novo lider.
                if replacement[:self.store.commit_index] != self.store.log[:self.store.commit_index]:
                    raise RuntimeError("Tentativa de alterar prefixo comitado.")
                old_commit = self.store.commit_index
                diverged = any(a != b for a, b in zip(self.store.log, replacement))
                truncated = diverged or len(replacement) < len(self.store.log)
                changed = replacement != self.store.log
                self.store.log = replacement
                self.store.commit_index = max(
                    old_commit, min(response.commit_index, len(replacement))
                )
                if changed or old_commit != self.store.commit_index:
                    self.store.salvar()
                if truncated:
                    self.truncations += 1
                    self.registrar_evento(f"TRUNCAMENTO: prefixo comum ate {cursor}; log corrigido via Fetch")
                elif changed or old_commit != self.store.commit_index:
                    self.registrar_evento("Fetch: log persistido / commit aplicado")
                self.changed.notify_all()
                return

    async def descobrir_lider(self):
        async def consultar_estado_do_no(peer, stub):
            try:
                return peer, await stub.Status(pb.Empty(), timeout=self.cfg.rpc_timeout)
            except grpc.aio.AioRpcError as error:
                LOG.debug("Status no=%s destino=%s erro=%s", self.id, peer, error.code())
                return peer, None

        responses = await asyncio.gather(*(consultar_estado_do_no(p, s) for p, s in self.peers.items()))
        async with self.changed:
            for _, response in responses:
                if response is not None:
                    self.observar_termo(response.term)
            leaders = [(peer, r) for peer, r in responses if r is not None
                       and r.role == "lider" and r.term == self.store.term]
            if leaders and self.role != "lider":
                self.leader_id = leaders[0][0]
            else:
                self.leader_id = 0

    async def executar(self):
        last_report = 0.0
        while True:
            if self.role == "lider":
                active = self.obter_replicas_ativas()
                if active != self.reported_active:
                    self.registrar_evento(f"replicas ativas={sorted(active)} quorum={len(active) + 1}/4")
                    self.reported_active = active
            else:
                if not self.fetch_paused:
                    if not self.leader_id:
                        await self.descobrir_lider()
                    if self.leader_id in self.peers:
                        await self.buscar_do_lider(self.leader_id)
                if time.monotonic() >= self.deadline:
                    await self.iniciar_eleicao()
            if time.monotonic() - last_report >= 2:
                self.registrar_evento("estado periodico")
                last_report = time.monotonic()
            await asyncio.sleep(self.cfg.fetch_interval)


async def iniciar_servidor(args):
    cfg = Settings.carregar(args.config)
    if args.id not in cfg.nodes:
        raise ValueError("Identificador de no invalido.")
    storage = Storage(Path(args.data_dir) / f"node-{args.id}.sqlite3")
    node = Node(args.id, cfg, storage, args.enable_test_controls)
    server = grpc.aio.server()
    rpc.add_ClusterServicer_to_server(node, server)
    server.add_insecure_port(cfg.nodes[args.id])
    await server.start()
    node.registrar_evento(f"gRPC escutando em {cfg.nodes[args.id]}")
    worker = asyncio.create_task(node.executar())
    termination = asyncio.create_task(server.wait_for_termination())
    try:
        done, _ = await asyncio.wait([worker, termination], return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            task.result()
    finally:
        worker.cancel()
        termination.cancel()
        await asyncio.gather(worker, termination, return_exceptions=True)
        await server.stop(0)
        await asyncio.gather(*(channel.close() for channel in node.channels.values()))
        storage.fechar()


def principal():
    parser = argparse.ArgumentParser(description="No de replicacao Pull gRPC")
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--enable-test-controls", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(iniciar_servidor(args))
    except KeyboardInterrupt:
        LOG.info("No encerrado pelo operador.")


if __name__ == "__main__":
    principal()
