# Replicacao Pull, eleicao e tolerancia a falhas com gRPC

Implementacao do trabalho de Sistemas Distribuidos: quatro processos de nos
com o mesmo codigo e um processo de cliente. Python 3.11+, gRPC/Protocol Buffers
e SQLite. O banco final e um mapa chave/valor com operacao de insercao/atualizacao.

## Executar no Windows

No PowerShell, dentro desta pasta:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\start.ps1
```

O ambiente `.venv` ja foi preparado durante a implementacao. O script abre quatro
processos de servidor em janelas independentes e executa o cliente no terminal
atual. Aguarde cerca de 2 segundos pela eleicao. Nao execute dois clusters nas
mesmas portas. Sair do cliente nao encerra os servidores.

Alternativa sem o script: execute um comando em cada um de quatro terminais,
substituindo o identificador por `1`, `2`, `3` e `4`:

```powershell
.\.venv\Scripts\python.exe -u node.py --id 1
```

No quinto terminal:

```powershell
.\.venv\Scripts\python.exe client.py
```

### Comandos do cliente

```text
status
put 1 cidade Curitiba
get 1 cidade
getall cidade
put 2 mensagem "replicacao com grpc"
sleep 1
getall mensagem
help
exit
```

`put` pode comecar em qualquer no: seguidores/candidatos retornam `NOT_LEADER`
e o identificador do lider conhecido; o cliente segue esse redirecionamento.
Durante uma eleicao o lider pode ser desconhecido (`0`); consulte `status` e tente
novamente. Uma lideranca recem-eleita precisa primeiro receber Fetches para
conhecer a atividade do quorum.

`get` consulta estritamente o no escolhido, sem encaminhar a leitura ao lider.
`getall` consulta os quatro nos individualmente. As respostas exibem `found`,
`value`, `term` e `commit_index`. Leitura em seguidor pode estar atrasada.
Os logs dos servidores exibem papel, termo, commit, tamanho do log, votos,
atividade das replicas e truncamentos.

Tambem e possivel executar um comando sem abrir o modo interativo:

```powershell
.\.venv\Scripts\python.exe client.py status
.\.venv\Scripts\python.exe client.py put 2 exemplo valor
.\.venv\Scripts\python.exe client.py getall exemplo
```

## Arquitetura e protocolo

- [cluster.proto](./cluster.proto): contrato de todas as chamadas.
- [node.py](./node.py): servidor identico nos quatro processos, eleicao e Pull.
- [storage.py](./storage.py): persistencia transacional e banco final comitado.
- [client.py](./client.py): cliente gRPC com redirecionamento e leituras por no.
- [settings.py](./settings.py) e [config.json](./config.json): configuracao validada.
- [CHECKLIST.md](./CHECKLIST.md): verificacao rastreavel do enunciado.

Todas as comunicacoes cliente/no e no/no usam exclusivamente gRPC:

| RPC | Finalidade |
|---|---|
| `RequestVote` | Eleicao com termo, ultimo indice e ultimo termo do candidato |
| `Fetch` | Seguidor solicita dados, informa prefixo persistido e recebe commit |
| `Write` | Escrita no lider, com resposta de sucesso somente depois do commit |
| `Read` | Consulta somente o banco final comitado do no local |
| `Status` | Descoberta de lider e observacao de papel, termo, commit e replicas |
| `Control` | Controles de teste; rejeitados sem habilitacao explicita |

Nao existe `AppendEntries`, envio ativo de dados pelo lider, REST ou banco
compartilhado. `Status` descobre o lider quando necessario, mas **nao substitui
Fetch como heartbeat** e nao renova o temporizador de eleicao por simples
recebimento de um status de mesmo termo.

gRPC-Web nao e necessario: o enunciado permite cliente de terminal. A variante
para navegador exigiria uma camada de traducao/proxy adicional; aqui cliente e
servidores utilizam gRPC nativo, preservando a arquitetura de cinco processos.

### Eleicao

O no inicia como seguidor. O prazo aleatorio de eleicao e de 1,0 a 1,8 segundos;
um Fetch bem-sucedido o reinicia. Apos expirar, o no vira candidato, incrementa
e persiste o termo, vota em si mesmo e solicita os outros votos via gRPC.
Sao necessarios **3 votos de um total fixo de 4 nos**.

Voto concedido e persistido antes da resposta e nao pode ser trocado no mesmo
termo. A atualizacao do log e comparada lexicograficamente por
`(ultimo_termo, ultimo_indice)`, conforme Raft. Termo maior em requisicao ou
resposta de eleicao/replicacao/descoberta e persistido e provoca transicao para
seguidor, inclusive se o receptor era lider.

Cada novo lider acrescenta uma entrada interna `noop` do termo atual. Ela nao
altera o banco chave/valor e permite efetivar com seguranca um prefixo de termos
anteriores: o lider so avanca diretamente o commit com uma entrada do seu
proprio termo. Por isso, indices incluem entradas internas e nao equivalem ao
numero de escritas do usuario.

### Replicacao, confirmacao e quorum

1. A replica envia Fetch aproximadamente a cada 100 ms, com termo atual,
   identificador e indice/termo do ultimo prefixo que possui em disco.
2. O lider registra o instante do Fetch de cada replica. Depois de 700 ms
   sem Fetch, considera-a inativa e exibe a mudanca nos logs.
3. O lider rejeita novas escritas com `NO_QUORUM` se nao conhecer pelo menos
   duas replicas ativas, sem sequer acrescentar a operacao ao log.
4. Uma escrita aceita recebe indice sequencial e termo atual e e persistida
   no log local. Isso ainda **nao** confirma sucesso ao cliente.
5. A resposta de Fetch transporta ate 32 entradas e o commit atual. Lotes
   limitados evitam exceder o tamanho padrao de mensagens gRPC; cada valor
   pode ter ate 65.536 bytes UTF-8 e a chave, ate 256 bytes.
6. A replica persiste as entradas. Somente seu **proximo Fetch** confirma
   ao lider o prefixo efetivamente armazenado.
7. O lider calcula o terceiro maior indice confirmado, incluindo seu proprio
   log; se a regra do termo permitir, persiste o commit e aplica o prefixo
   ao banco final. Entao libera a resposta de sucesso ao cliente.
8. Cada replica so aplica esse commit ao receber uma resposta de Fetch com
   o indice atualizado. Entradas apenas recebidas nunca aparecem em `Read`.

Uma escrita pode aguardar ate 4 segundos. Perda de lideranca, deadline ou queda
da conexao apos append deixam o resultado **indeterminado**, nao equivalem a
rollback e nao sao apresentados como sucesso. Uma entrada nao confirmada pode
ser comitada posteriormente ou removida por um novo lider. O cliente nao repete
automaticamente escritas ambiguas; consulte o estado antes de decidir.

### Correcao de divergencias

Se o indice/termo informado nao corresponder ao log do lider, a resposta de
Fetch informa um indice anterior para nova tentativa. A replica recua apenas
o cursor de busca, sem apagar seu log especulativamente. Apos localizar um
prefixo comum, substitui a cauda pela resposta do lider e persiste a correcao.
Um prefixo ja comitado nunca pode ser truncado; essa violacao causa erro explicito.

### Persistencia

Cada no usa seu proprio `data\node-N.sqlite3`, com WAL e `synchronous=FULL`:

- `metadata`: termo, `voted_for` e indice de commit;
- `log`: indice, termo, chave, valor e indicador de entrada interna;
- `kv`: somente o estado final comitado.

Atualizacoes de metadados, log e banco final sao atomicas em uma transacao.
O reinicio usa o mesmo arquivo, recupera termo/voto/log/commit/dados e sempre
inicia como seguidor. Nao exclua nem edite arquivos SQLite ou seus arquivos
auxiliares enquanto os nos estiverem executando.

Esta implementacao didatica regrava o estado em cada transacao para simplificar
a consistencia; nao implementa compactacao de log, snapshots, mudanca de membros,
TLS, autenticacao nem deduplicacao/exatamente-uma-vez. As portas padrao estao
restritas ao loopback. Nao e uma implementacao Raft de producao.

## Testes e apresentacao

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```

Os testes utilizam portas livres e diretorios temporarios independentes de
`data`, iniciam quatro subprocessos reais e fazem todas as interacoes por gRPC.
Encerram somente os processos que eles mesmos criaram. Inspecoes locais de
SQLite nos testes comprovam persistencia; nao fazem parte da comunicacao entre
os nos.

Resultado verificado: **18 testes passaram**, incluindo os cinco cenarios
obrigatorios (com ambos os subcenarios de recuperacao) e reinicio completo.
Veja as evidencias por requisito no [checklist](./CHECKLIST.md).

### Roteiro manual de defesa

1. **Normal/redirecionamento:** `status`; escolha um seguidor; execute
   `put NO chave valor`, observe o redirecionamento, aguarde e use `getall chave`.
2. **Queda do lider:** identifique-o por `status`, feche apenas sua janela,
   aguarde e consulte `status` para mostrar novo lider e termo maior.
3. **Perda de quorum:** com todos ligados, feche dois seguidores; apos o
   heartbeat timeout, escreva no lider e mostre `NO_QUORUM`.
4. **Replica atrasada:** mantenha lider e dois seguidores; desligue o quarto,
   realize escritas, reinicie-o com o mesmo `--id` e `--data-dir` e mostre
   `getall` e os logs de Fetch.
5. **Divergencia e isolamento:** execute os testes especificos abaixo. Os
   controles deterministas permitem comprovar condicoes que duram milissegundos.

Para reiniciar manualmente um no, execute seu comando original, sem apagar `data`.
Para encerrar todo o cluster manual, use Ctrl+C/feche as quatro janelas.

### Controles deterministas, somente para testes

Habilite explicitamente com `.\start.ps1 -EnableTestControls` ou com
`--enable-test-controls` em cada servidor. Sem a flag, `Control` e
`crash_after_append` retornam `PERMISSION_DENIED`.

- `crash-put NO chave valor`: o lider persiste a entrada e termina com codigo
  86 **antes** de o loop atender qualquer Fetch dessa entrada. Aguarde a eleicao,
  escreva no novo lider e reinicie o antigo para observar `TRUNCAMENTO`.
- `pause-commit NO on|off`: suspende apenas o avanco de commit no lider,
  sem interromper Fetches ou eleicoes; permite demonstrar dados recebidos
  pelas replicas ainda invisiveis. E um controle de laboratorio, nao protocolo
  normal. No teste, uma thread do mesmo cliente aguarda a escrita enquanto
  outra libera o commit apos verificar a invisibilidade.
- `pause-fetch NO on|off`: suspende a busca desse no; o temporizador de
  eleicao continua funcionando normalmente.

Execute separadamente os cenarios mais delicados:

```powershell
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_integration.py -k test_04b -v
.\.venv\Scripts\python.exe -m unittest discover -s tests -p test_integration.py -k test_05 -v
```

A defesa oral e a explicacao do codigo sao obrigacoes do aluno e **nao sao
substituidas por estes testes**.

## Gerar novamente os stubs

Os arquivos `cluster_pb2.py`, `cluster_pb2.pyi` e `cluster_pb2_grpc.py` sao
gerados, nao editados manualmente. Apos alterar o contrato:

```powershell
.\.venv\Scripts\python.exe -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. cluster.proto
```

## Referencias fornecidas

- https://grpc.io/docs/languages/
- https://grpc.io/docs/platforms/web/
- https://grpc.io/blog/state-of-grpc-web/
- Tutorial utilizado para a integracao Python:
  https://grpc.io/docs/languages/python/quickstart/
