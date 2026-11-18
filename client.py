import argparse
import json
import shlex
import time

import grpc
from google.protobuf.json_format import MessageToDict

import cluster_pb2 as pb
import cluster_pb2_grpc as rpc
from settings import Settings


class Client:
    def __init__(self, settings: Settings):
        self.cfg = settings
        self.channels = {node: grpc.insecure_channel(address)
                         for node, address in settings.nodes.items()}
        self.stubs = {node: rpc.ClusterStub(channel)
                      for node, channel in self.channels.items()}

    def fechar(self):
        for channel in self.channels.values():
            channel.close()

    def consultar_estados(self):
        result = {}
        for node, stub in self.stubs.items():
            try:
                result[node] = stub.Status(pb.Empty(), timeout=self.cfg.rpc_timeout)
            except grpc.RpcError as error:
                print(f"No {node}: indisponivel ({error.code().name})")
        return result

    def gravar(self, node: int, key: str, value: str, crash: bool = False):
        request = pb.WriteRequest(key=key, value=value, crash_after_append=crash)
        # Impede seguir indefinidamente indicacoes de lider que formem um ciclo.
        visited = set()
        while node not in visited:
            visited.add(node)
            response = self.stubs[node].Write(request, timeout=self.cfg.write_timeout + 1)
            # So repete quando sabe que o no recusou por nao ser o lider.
            # Timeout ou queda apos append podem ter resultado indeterminado.
            if response.error != "NOT_LEADER":
                return response
            leader = response.leader_id
            if leader not in self.stubs:
                return response
            print(f"Redirecionamento gRPC: no {node} -> lider {leader}")
            node = leader
        raise RuntimeError("Ciclo de redirecionamento; eleicao em andamento. Consulte status.")


HELP = """
status                         Papel, termo, commit e atividade dos nos
put NO CHAVE VALOR             Grava, seguindo redirecionamento ao lider
get NO CHAVE                   Le apenas o banco comitado do no escolhido
getall CHAVE                   Consulta o dado nos quatro nos
sleep SEGUNDOS                 Aguarda convergencia
pause-fetch NO on|off          Controle de teste, exige flag no servidor
pause-commit NO on|off         Controle de teste, exige flag no servidor
crash-put NO CHAVE VALOR       TESTE: crash do lider apos append persistido
help                           Exibe comandos
exit                           Encerra somente o cliente
Valores com espacos podem ser colocados entre aspas.
"""


def exibir_resposta(message):
    print(json.dumps(MessageToDict(message, preserving_proto_field_name=True,
                                   always_print_fields_with_no_presence=True),
                     indent=2, ensure_ascii=False))


def executar_comando(client: Client, words: list[str]):
    command, *args = words
    if command == "help" and not args:
        print(HELP)
    elif command == "status" and not args:
        for status in client.consultar_estados().values():
            exibir_resposta(status)
    elif command in ("put", "crash-put") and len(args) == 3:
        exibir_resposta(client.gravar(int(args[0]), args[1], args[2], command == "crash-put"))
    elif command == "get" and len(args) == 2:
        # Read consulta somente o no escolhido; nao segue redirecionamento.
        exibir_resposta(client.stubs[int(args[0])].Read(
            pb.ReadRequest(key=args[1]), timeout=client.cfg.rpc_timeout
        ))
    elif command == "getall" and len(args) == 1:
        for node, stub in client.stubs.items():
            try:
                print(f"No {node}:")
                exibir_resposta(stub.Read(pb.ReadRequest(key=args[0]), timeout=client.cfg.rpc_timeout))
            except grpc.RpcError as error:
                print(f"Indisponivel: {error.code().name}")
    elif command == "sleep" and len(args) == 1:
        seconds = float(args[0])
        if seconds < 0:
            raise ValueError("Tempo deve ser nao negativo.")
        time.sleep(seconds)
    elif command in ("pause-fetch", "pause-commit") and len(args) == 2:
        if args[1] not in ("on", "off"):
            raise ValueError("Use on ou off.")
        field = "pause_fetch" if command == "pause-fetch" else "pause_commit"
        exibir_resposta(client.stubs[int(args[0])].Control(
            pb.ControlRequest(**{field: args[1] == "on"}), timeout=client.cfg.rpc_timeout
        ))
    else:
        raise ValueError("Comando/argumentos invalidos. Use help.")


def principal():
    parser = argparse.ArgumentParser(description="Cliente gRPC interativo")
    parser.add_argument("--config", default="config.json")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    client = Client(Settings.carregar(args.config))
    try:
        if args.command:
            executar_comando(client, args.command)
            return
        print(HELP)
        while True:
            try:
                words = shlex.split(input("grpc> "))
                if words == ["exit"]:
                    break
                if words:
                    executar_comando(client, words)
            except (ValueError, KeyError, RuntimeError) as error:
                print(f"Erro: {error}")
            except grpc.RpcError as error:
                print(f"Falha gRPC: {error.code().name}: {error.details()}. "
                      "Escrita sem resposta tem resultado indeterminado; consulte getall.")
            except EOFError:
                break
    finally:
        client.fechar()


if __name__ == "__main__":
    try:
        principal()
    except KeyboardInterrupt:
        print("\nCliente encerrado.")
    except (ValueError, KeyError, RuntimeError, grpc.RpcError) as error:
        raise SystemExit(f"Erro: {error}") from error
