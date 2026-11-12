# Checklist de requisitos e verificacao

Enunciado: quatro imagens fornecidas na pasta do trabalho, paginas 0 a 3.
Verificacao realizada no Windows com Python 3.11.9 em 11/11/2026.

## Resultado executado

```text
python -m unittest discover -s tests -v
Ran 18 tests in 37.189s
OK
```

Comando executado com o Python do ambiente `.venv`. Sao **7 testes de integracao**
com quatro processos reais e **11 testes unitarios**. Tambem passaram:
`pip check`, compilacao de sintaxe Python, ajuda dos dois executaveis e analise
de sintaxe PowerShell do inicializador. O inicializador foi validado
sintaticamente; a execucao dos servidores foi validada pelos testes de integracao.

## Requisitos gerais

- [x] Quatro nos com o mesmo codigo e papeis dinamicos: [node.py](./node.py).
- [x] Um cliente com interface de terminal: [client.py](./client.py).
- [x] Inicializacao dos quatro nos e cliente: [start.ps1](./start.ps1).
- [x] Comunicacao exclusivamente via gRPC: [cluster.proto](./cluster.proto);
  RPCs reais utilizadas por todos os testes de integracao.
- [x] Modelo Pull; nao ha replicacao Push nem heartbeat enviado pelo lider.
- [x] Exibicao de papel, termo e indice de commit em logs e `status`.
- [x] Parametros de execucao, persistencia e roteiro de apresentacao documentados
  no [README](./README.md).

## Itens pontuados: cliente (0,30)

| Estado | Requisito | Evidencia |
|---|---|---|
| [x] | Gravacao com redirecionamento (0,15) | `test_01_normal_write_redirection_distributed_read`: verifica `NOT_LEADER`, identificador do lider e sucesso apos redirecionar |
| [x] | Consulta em qualquer no (0,15) | Mesmo teste consulta explicitamente os quatro nos; `get` e `getall` no cliente |

## Itens pontuados: deteccao de falhas e eleicao (0,60)

| Estado | Requisito | Evidencia |
|---|---|---|
| [x] | Temporizador aleatorio e transicoes seguidor/candidato/lider (0,25) | `reset_election`, `campaign`, `pull`; `test_02_leader_crash_new_term_and_durable_commit` mata o lider e aguarda outro |
| [x] | Incremento de termo, RequestVote e voto unico (0,25) | `test_one_vote_per_term_survives_restart`; termo/voto persistidos antes de responder |
| [x] | Restricao de voto pela atualizacao do log (0,10) | `test_log_freshness_uses_term_then_index` verifica rejeicao por termo antigo e por indice menor |
| [x] | Termo maior obriga retorno a seguidor | `test_higher_term_demotes_leader_even_in_fetch`, `test_higher_term_in_vote_response_cancels_election` e `test_higher_term_in_fetch_response_is_persisted` |
| [x] | Quorum fixo de 3 em 4 para eleicao | `QUORUM = 3`, contagem em `campaign`; teste de queda e nova eleicao com exatamente tres sobreviventes |

## Itens pontuados: lider e replicacao Pull (0,80)

| Estado | Requisito | Evidencia |
|---|---|---|
| [x] | Gravacao local com termo e indice sequencial (0,20) | `Write` e teste `test_04b_divergent_uncommitted_suffix_is_truncated`, que inspeciona em disco a entrada apos crash |
| [x] | Atendimento de Fetch e controle de quorum (0,20) | `test_fetch_only_acknowledges_previously_stored_entries` exige novo Fetch para confirmar recebimento; `test_commit_requires_three_acks_and_current_term` exige tres copias |
| [x] | Commit informado nas respostas de Fetch | Mesmo teste verifica que os Fetches iniciais recebem commit 0 e a confirmacao posterior recebe commit 1 |
| [x] | Monitoramento individual de inatividade | `last_fetch` por replica, `active_peers`; teste unitario expira uma replica sem expirar outra; `test_03_quorum_loss_rejects_write_without_append` verifica efeito real |
| [x] | Sem quorum, sem escrita | Testes unitario e de integracao comprovam `NO_QUORUM`, sem append nem avanco de commit |
| [x] | Confirmacao ao cliente somente apos commit (0,20) | `test_05_fetched_uncommitted_data_is_invisible` comprova que a escrita fica pendente antes do commit e so entao retorna sucesso |
| [x] | Persistencia em disco (0,20) | `test_persistence_and_commit_visibility`, `test_one_vote_per_term_survives_restart` e `test_06_all_nodes_restart_preserves_data_and_term` |

## Itens pontuados: replicas e inconsistencias (0,80)

| Estado | Requisito | Evidencia |
|---|---|---|
| [x] | Loop ativo de Fetch e aplicacao de commit (0,20) | Testes 01, 04a e 05; Fetch a cada aproximadamente 100 ms e aplicacao somente do commit recebido |
| [x] | Truncamento e correcao de divergencias (0,30) | `test_04b_divergent_uncommitted_suffix_is_truncated` exige contador de truncamentos, remove entrada divergente e converge |
| [x] | Cauda extra e lider com log mais curto | `test_pull_backtracks_and_removes_extra_suffix` verifica recuo ate indice 0 e remocao segura da cauda |
| [x] | Leitura eventual apenas de dados comitados (0,15) | `test_05_fetched_uncommitted_data_is_invisible` e teste de persistencia com duas versoes da mesma chave |
| [x] | Persistencia local (0,15) | SQLite separado por no; teste 06 mata e reinicia todos, verifica dados e termos recuperados |

## Cenarios obrigatorios da apresentacao

| Estado | Cenario | Teste executado |
|---|---|---|
| [x] | 1. Escrita normal e leitura distribuida | `test_01_normal_write_redirection_distributed_read` |
| [x] | 2. Queda do lider, eleicao automatica e termo maior | `test_02_leader_crash_new_term_and_durable_commit` |
| [x] | 3. Menos de tres ativos bloqueia/rejeita escrita | `test_03_quorum_loss_rejects_write_without_append` |
| [x] | 4A. Replica desligada recupera escritas faltantes | `test_04a_offline_follower_catches_up_via_fetch` |
| [x] | 4B. Crash apos append, novo lider, nova escrita, retorno e truncamento do antigo | `test_04b_divergent_uncommitted_suffix_is_truncated` |
| [x] | 5. Entrada recebida via Fetch permanece invisivel ate commit | `test_05_fetched_uncommitted_data_is_invisible` |
| [x] | Extra: reinicio simultaneo de todos os nos | `test_06_all_nodes_restart_preserves_data_and_term` |
| [x] | Extra: lotes de Fetch abaixo do limite padrao de mensagem gRPC | `test_fetch_batches_large_logs` |

Os controles `crash_after_append` e `pause_commit` sao habilitados somente nos
servidores de teste. Eles tornam os cenarios 4B e 5 deterministas, sem depender
de encerrar manualmente um processo dentro de uma janela de poucos milissegundos.
No cenario 5, os arquivos locais tambem sao inspecionados para provar que a
entrada foi realmente persistida em todos os logs, nao apenas que a leitura
retornou ausente.

## Persistencia obrigatoria

- [x] Termo atual e voto concedido: tabela `metadata`.
- [x] Log com termo e indice: tabela `log`.
- [x] Banco final somente com dados comitados: tabela `kv`.
- [x] Indice de commit persistido e consistente com a aplicacao ao banco.
- [x] Transacao atomica com SQLite WAL e `synchronous=FULL`.
- [x] Arquivos independentes por no; reinicio sem apagar o estado.

## Pendencias externas e limites

- [ ] Realizar a defesa oral e explicar o codigo a professora.
- [ ] Executar/demonstrar os cenarios durante a apresentacao.
- [ ] Informar autoria individual ou dupla conforme a entrega da disciplina.

Todos os requisitos tecnicos identificados no enunciado foram implementados e
verificados conforme as evidencias acima. Isso nao certifica ausencia de bugs
sob qualquer falha ou carga e nao substitui a avaliacao da professora.
Os testes usam falhas por encerramento abrupto de processo, nao queda fisica de
energia nem particoes reais de rede. Autenticacao, TLS, compactacao e alteracao
dinamica de membros nao fazem parte do escopo solicitado.
