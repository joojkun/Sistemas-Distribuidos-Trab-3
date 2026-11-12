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
        self.reset_election()

    def reset_election(self) -> None:
        self.deadline = time.monotonic() + random.uniform(
            self.cfg.election_min, self.cfg.election_max
        )

    def report(self, event: str) -> None:
        LOG.info("no=%s papel=%s termo=%s commit=%s log=%s %s",
                 self.id, self.role, self.store.term, self.store.commit_index,
                 len(self.store.log), event)

    def observe_term(self, term: int) -> None:
        if term > self.store.term:
            self.store.term = term
            self.store.voted_for = None
            self.store.save()
            self.role = "seguidor"
            self.leader_id = 0
            self.last_fetch.clear()
            self.matches.clear()
            self.reset_election()
            self.changed.notify_all()
            self.report("termo maior recebido; transicao para seguidor")

    def active_peers(self) -> set[int]:
        now = time.monotonic()
        return {peer for peer, instant in self.last_fetch.items()
                if now - instant <= self.cfg.heartbeat_timeout}

    def advance_commit(self) -> None:
        if self.role != "lider" or self.commit_paused:
            return
        confirmed = sorted([len(self.store.log)] + [
            self.matches.get(peer, 0) for peer in self.peers
        ], reverse=True)[QUORUM - 1]
        # Raft only commits a previous-term prefix through a current-term entry.
        if (confirmed > self.store.commit_index
                and self.store.term_at(confirmed) == self.store.term):
            self.store.commit_index = confirmed
            self.store.save()
            self.changed.notify_all()
            self.report("commit confirmado por quorum")

    async def RequestVote(self, request, context):
        if request.candidate_id not in self.peers:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Candidato invalido.")
        async with self.changed:
            self.observe_term(request.term)
            up_to_date = (request.last_term, request.last_index) >= (
                self.store.term_at(len(self.store.log)), len(self.store.log)
            )
            granted = (
                request.term == self.store.term and up_to_date
                and self.store.voted_for in (None, request.candidate_id)
            )
            if granted:
                self.store.voted_for = request.candidate_id
                self.store.save()
                self.reset_election()
                self.report(f"voto concedido a {request.candidate_id}")
            return pb.VoteResponse(term=self.store.term, granted=granted)

    async def Fetch(self, request, context):
        if request.follower_id not in self.peers or request.last_index < 0:
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Fetch invalido.")
        async with self.changed:
            self.observe_term(request.term)
            response = pb.FetchResponse(
                term=self.store.term,
                leader_id=self.id if self.role == "lider" else self.leader_id,
            )
            if self.role != "lider" or request.term != self.store.term:
                return response
            self.last_fetch[request.follower_id] = time.monotonic()
            index = request.last_index
            if index > len(self.store.log) or self.store.term_at(index) != request.last_term:
                response.retry_index = min(max(0, index - 1), len(self.store.log))
                return response
            self.matches[request.follower_id] = index
            self.advance_commit()
            response.success = True
            response.entries.extend(self.store.log[index:index + 32])
            response.commit_index = self.store.commit_index
            return response

    async def Write(self, request, context):
        if (not request.key or len(request.key.encode("utf-8")) > 256
                or len(request.value.encode("utf-8")) > 65536):
            await context.abort(grpc.StatusCode.INVALID_ARGUMENT, "Chave/valor invalidos.")
        if request.crash_after_append and not self.test_controls:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Controles de teste desativados.")
        async with self.changed:
            def result(error: str = "", committed: bool = False, index: int = 0):
                return pb.WriteResponse(
                    term=self.store.term, leader_id=self.leader_id,
                    committed=committed, index=index, error=error,
                )
            if self.role != "lider":
                return result("NOT_LEADER")
            if len(self.active_peers()) + 1 < QUORUM:
                self.report("escrita recusada: NO_QUORUM")
                return result("NO_QUORUM")
            term = self.store.term
            index = len(self.store.log) + 1
            entry = pb.Entry(index=index, term=term, key=request.key, value=request.value)
            self.store.log.append(entry)
            self.store.save()
            self.report(f"append local indice={index} chave={request.key}")
            if request.crash_after_append:
                self.report("TESTE: crash apos persistencia e antes da replicacao")
                # The event loop cannot serve a Fetch before this durable append crashes.
                os._exit(86)
            deadline = time.monotonic() + self.cfg.write_timeout
            while True:
                if self.role != "lider" or self.store.term != term:
                    return result("LEADERSHIP_CHANGED; resultado indeterminado", index=index)
                if self.store.commit_index >= index:
                    return result(committed=True, index=index)
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    self.report(f"escrita sem confirmacao indice={index}")
                    return result("COMMIT_TIMEOUT; resultado indeterminado", index=index)
                try:
                    await asyncio.wait_for(self.changed.wait(), remaining)
                except asyncio.TimeoutError:
                    return result("COMMIT_TIMEOUT; resultado indeterminado", index=index)

    async def Read(self, request, context):
        async with self.changed:
            value = self.store.read(request.key)
            return pb.ReadResponse(term=self.store.term, found=value is not None,
                                   value=value if value is not None else "",
                                   commit_index=self.store.commit_index)

    def status(self):
        active = self.active_peers()
        return pb.StatusResponse(
            node_id=self.id, role=self.role, term=self.store.term,
            leader_id=self.leader_id, commit_index=self.store.commit_index,
            last_index=len(self.store.log), fetch_paused=self.fetch_paused,
            commit_paused=self.commit_paused, truncations=self.truncations,
            peers=[pb.PeerStatus(node_id=peer, active=peer in active,
                                 match_index=self.matches.get(peer, 0))
                   for peer in self.peers],
        )

    async def Status(self, request, context):
        return self.status()

    async def Control(self, request, context):
        if not self.test_controls:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Controles de teste desativados.")
        async with self.changed:
            if request.HasField("pause_fetch"):
                self.fetch_paused = request.pause_fetch
            if request.HasField("pause_commit"):
                self.commit_paused = request.pause_commit
                self.advance_commit()
            self.report(f"TESTE: fetch_paused={self.fetch_paused} commit_paused={self.commit_paused}")
            return self.status()

    async def campaign(self):
        async with self.changed:
            self.role = "candidato"
            self.leader_id = 0
            self.store.term += 1
            self.store.voted_for = self.id
            self.store.save()
            self.reset_election()
            term = self.store.term
            request = pb.VoteRequest(
                term=term, candidate_id=self.id, last_index=len(self.store.log),
                last_term=self.store.term_at(len(self.store.log)),
            )
            self.report("iniciando eleicao")

        async def vote(peer, stub):
            try:
                return await stub.RequestVote(request, timeout=self.cfg.rpc_timeout)
            except grpc.aio.AioRpcError as error:
                LOG.debug("RequestVote no=%s destino=%s erro=%s", self.id, peer, error.code())
                return None

        responses = await asyncio.gather(*(vote(p, s) for p, s in self.peers.items()))
        async with self.changed:
            for response in responses:
                if response is not None:
                    self.observe_term(response.term)
            if self.store.term != term or self.role != "candidato":
                return
            if 1 + sum(r is not None and r.term == term and r.granted for r in responses) >= QUORUM:
                self.role = "lider"
                self.leader_id = self.id
                self.last_fetch.clear()
                self.matches.clear()
                self.reported_active.clear()
                self.store.log.append(pb.Entry(
                    index=len(self.store.log) + 1, term=term, noop=True
                ))
                self.store.save()
                self.report("eleito com pelo menos 3 votos; noop do termo acrescentado")

    async def pull(self, leader: int):
        async with self.changed:
            term = self.store.term
            cursor = len(self.store.log)
        # Backtrack only the probe cursor; never delete a durable prefix speculatively.
        while True:
            async with self.changed:
                if self.role == "lider" or self.store.term != term or self.fetch_paused:
                    return
                request = pb.FetchRequest(
                    term=term, follower_id=self.id, last_index=cursor,
                    last_term=self.store.term_at(cursor),
                )
            try:
                response = await self.peers[leader].Fetch(
                    request, timeout=self.cfg.rpc_timeout
                )
            except grpc.aio.AioRpcError as error:
                LOG.debug("Fetch no=%s destino=%s erro=%s", self.id, leader, error.code())
                return
            async with self.changed:
                self.observe_term(response.term)
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
                self.reset_election()
                incoming = list(response.entries)
                if any(e.index != cursor + i + 1 for i, e in enumerate(incoming)):
                    raise RuntimeError("Resposta Fetch com indices invalidos.")
                replacement = self.store.log[:cursor] + incoming
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
                    self.store.save()
                if truncated:
                    self.truncations += 1
                    self.report(f"TRUNCAMENTO: prefixo comum ate {cursor}; log corrigido via Fetch")
                elif changed or old_commit != self.store.commit_index:
                    self.report("Fetch: log persistido / commit aplicado")
                self.changed.notify_all()
                return

    async def discover(self):
        async def status(peer, stub):
            try:
                return peer, await stub.Status(pb.Empty(), timeout=self.cfg.rpc_timeout)
            except grpc.aio.AioRpcError as error:
                LOG.debug("Status no=%s destino=%s erro=%s", self.id, peer, error.code())
                return peer, None

        responses = await asyncio.gather(*(status(p, s) for p, s in self.peers.items()))
        async with self.changed:
            for _, response in responses:
                if response is not None:
                    self.observe_term(response.term)
            leaders = [(peer, r) for peer, r in responses if r is not None
                       and r.role == "lider" and r.term == self.store.term]
            if leaders and self.role != "lider":
                self.leader_id = leaders[0][0]
            else:
                self.leader_id = 0

    async def run(self):
        last_report = 0.0
        while True:
            if self.role == "lider":
                active = self.active_peers()
                if active != self.reported_active:
                    self.report(f"replicas ativas={sorted(active)} quorum={len(active) + 1}/4")
                    self.reported_active = active
            else:
                if not self.fetch_paused:
                    if not self.leader_id:
                        await self.discover()
                    if self.leader_id in self.peers:
                        await self.pull(self.leader_id)
                if time.monotonic() >= self.deadline:
                    await self.campaign()
            if time.monotonic() - last_report >= 2:
                self.report("estado periodico")
                last_report = time.monotonic()
            await asyncio.sleep(self.cfg.fetch_interval)


async def serve(args):
    cfg = Settings.load(args.config)
    if args.id not in cfg.nodes:
        raise ValueError("Identificador de no invalido.")
    storage = Storage(Path(args.data_dir) / f"node-{args.id}.sqlite3")
    node = Node(args.id, cfg, storage, args.enable_test_controls)
    server = grpc.aio.server()
    rpc.add_ClusterServicer_to_server(node, server)
    server.add_insecure_port(cfg.nodes[args.id])
    await server.start()
    node.report(f"gRPC escutando em {cfg.nodes[args.id]}")
    worker = asyncio.create_task(node.run())
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
        storage.close()


def main():
    parser = argparse.ArgumentParser(description="No de replicacao Pull gRPC")
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--config", default="config.json")
    parser.add_argument("--data-dir", default="data")
    parser.add_argument("--enable-test-controls", action="store_true")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(serve(args))
    except KeyboardInterrupt:
        LOG.info("No encerrado pelo operador.")


if __name__ == "__main__":
    main()
