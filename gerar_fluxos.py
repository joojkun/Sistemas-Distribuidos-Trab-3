"""Gera um guia autonomo com trechos extraidos diretamente do projeto."""

import ast
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def criar_etapa(source, target, label, explanation, state, symbol, kind="request"):
    return dict(source=source, target=target, label=label, explanation=explanation,
                state=state, symbol=symbol, kind=kind)


def criar_fluxo(key, title, summary, example, actors, steps, alternatives):
    return dict(id=key, title=title, summary=summary, example=example,
                actors=actors, steps=steps, alternatives=alternatives)


FLOWS = [
    criar_fluxo("eleicao", "1. Eleição: escolher o líder",
         "Um nó fica tempo demais sem um Fetch bem-sucedido e pede votos. Precisa de 3 votos em 4.",
         "O nó 1 caiu. O nó 2 pode ser escolhido pelos nós 2, 3 e 4.",
         ["Nó 2 / candidato", "Disco do nó 2", "Nós 3 e 4"],
         [
             criar_etapa(0, 0, "Prazo aleatório expira", "Sem renovar o prazo por uma resposta de Fetch bem-sucedida, o nó inicia uma eleição. O prazo é sorteado entre 1 e 1,8 segundo.", "Seguidor → candidato", "node.py:reiniciar_prazo_eleicao", "local"),
             criar_etapa(0, 1, "Salva termo maior e voto em si", "O nó aumenta a rodada da eleição (termo) em 1 e registra seu próprio voto antes de pedir votos aos outros.", "Termo novo persistido", "node.py:iniciar_eleicao", "local"),
             criar_etapa(0, 2, "RequestVote: vocês votam em mim?", "O pedido leva termo, identificador e posição/termo da última entrada. As solicitações aos demais nós são concorrentes; a figura agrupa dois eleitores.", "Aguardando votos", "node.py:iniciar_eleicao"),
             criar_etapa(2, 2, "Confere o histórico e salva o voto", "O eleitor exige termo adequado, voto ainda disponível para esse candidato e log tão atualizado quanto o seu. Compara primeiro o último termo, depois o índice.", "No máximo um candidato por termo", "node.py:solicitar_voto", "local"),
             criar_etapa(2, 0, "Respostas: voto concedido", "Os nós 3 e 4 concedem seus votos neste exemplo. Somados ao voto próprio, são três.", "3 votos / 4 membros", "node.py:solicitar_voto", "response"),
             criar_etapa(0, 1, "Vira líder e salva uma noop", "Noop é uma entrada interna sem alteração de dados. Ela ajuda a confirmar com segurança o histórico de termos anteriores.", "Candidato → líder", "node.py:iniciar_eleicao", "local"),
         ],
         ["Sem três votos, não se torna líder; após outro prazo, pode tentar nova rodada.",
          "Uma resposta com termo maior cancela a candidatura e o nó vira seguidor.",
          "Vencer a eleição não basta para aceitar escritas: o líder precisa observar Fetches de pelo menos dois seguidores."]),
    criar_fluxo("busca", "2. Busca: Pull usando Fetch",
         "Pull é a estratégia de o seguidor buscar. Fetch é cada pedido concreto. A resposta do líder NÃO é Push.",
         "O seguidor tem até a entrada 4. O líder já tem a entrada 5: telefone = 9999.",
         ["Seguidor", "Líder", "Disco do seguidor"],
         [
             criar_etapa(0, 0, "Chega a hora de buscar", "O ciclo espera aproximadamente 100 ms entre iterações. Mesmo sem escrita nova, continua buscando.", "Estratégia Pull", "node.py:executar", "local"),
             criar_etapa(0, 1, "Fetch: tenho até 4; há novidades?", "O seguidor envia seu termo atual e o índice/termo da última entrada local. Este pedido é o Fetch que realiza o Pull.", "Confirma que possui até 4", "node.py:buscar_do_lider"),
             criar_etapa(1, 1, "Registra atividade e confere o índice", "O líder registra o horário da réplica e verifica se o ponto informado corresponde ao seu log. Só reconhece como armazenado o índice informado no pedido.", "Heartbeat + confirmação anterior", "node.py:atender_busca", "local"),
             criar_etapa(1, 0, "Resposta: entrada 5 + commit atual", "O líder devolve até 32 entradas faltantes e seu índice de commit. Enviar a entrada 5 ainda não prova que o seguidor a armazenou.", "Resposta a um pedido; não é Push", "node.py:atender_busca", "response"),
             criar_etapa(0, 2, "Persiste entrada e aplica commit recebido", "A réplica salva o log. Só aplica ao banco final as entradas autorizadas pelo commit desta resposta; as demais ficam invisíveis.", "Log pode estar à frente do banco", "node.py:buscar_do_lider", "local"),
             criar_etapa(0, 1, "Próximo Fetch: agora tenho até 5", "Só depois de salvar, uma nova busca informa o índice 5 ao líder. Esse novo Fetch confirma o armazenamento.", "Confirmação da entrada 5", "node.py:buscar_do_lider"),
         ],
         ["Se não houver novidade, a resposta pode trazer zero entradas; ainda assim é um Fetch e pode trazer commit atualizado.",
          "Se o líder mandar dados espontaneamente, seria Push. Não existe esse envio espontâneo neste projeto.",
          "Se o índice/termo divergir, começa o fluxo de truncamento, não uma confirmação."]),
    criar_fluxo("gravacao", "3. Gravação: do pedido ao sucesso",
         "Anotar no líder não basta. A escrita só retorna sucesso após confirmação de três cópias.",
         "Com líder no nó 1: put 1 telefone 9999. Dois seguidores são agrupados na figura.",
         ["Cliente", "Líder", "Seguidores 2 e 3", "Discos locais"],
         [
             criar_etapa(0, 1, "Write: telefone = 9999", "O cliente pede para guardar uma chave e um valor. Neste exemplo já acertou o líder.", "Pedido recebido", "client.py:gravar"),
             criar_etapa(1, 1, "Valida dados, papel e atividade", "O líder verifica a chave/valor e exige pelo menos dois seguidores com Fetch recente. Atividade permite tentar; não substitui confirmar cópias.", "Precisa de 3 nós contando o líder", "node.py:gravar", "local"),
             criar_etapa(1, 3, "Salva entrada 5 no próprio log", "A entrada recebe próximo índice e termo atual. O líder persiste no disco e aguarda, sem responder sucesso ainda.", "Pendente: commit ainda 4", "node.py:gravar", "local"),
             criar_etapa(2, 1, "Fetch: temos até 4", "Seguidores buscam o que falta por iniciativa própria. O líder não empurra a escrita.", "Pull", "node.py:atender_busca"),
             criar_etapa(1, 2, "Resposta: aqui está a entrada 5", "Cada seguidor recebe a operação no seu Fetch. As setas agrupam chamadas independentes.", "Recebida, ainda não confirmada", "node.py:atender_busca", "response"),
             criar_etapa(2, 3, "Seguidores salvam seus logs", "Cada seguidor usa seu próprio arquivo SQLite. Não existe disco compartilhado entre os nós.", "Três cópias físicas, líder ainda aguarda aviso", "node.py:buscar_do_lider", "local"),
             criar_etapa(2, 1, "Novos Fetches: temos até 5", "Os novos pedidos informam que a entrada foi armazenada. Com líder e dois seguidores, há três confirmações.", "Quórum de armazenamento", "node.py:atender_busca"),
             criar_etapa(1, 3, "Avança commit para 5 e aplica", "O terceiro maior índice confirmado determina o candidato a commit. O avanço direto exige entrada do termo atual.", "Operação oficialmente confirmada", "node.py:avancar_commit", "local"),
             criar_etapa(1, 0, "WriteResponse: committed = true", "Agora o cliente recebe sucesso. Um seguidor ainda pode precisar de outro Fetch para conhecer o commit 5.", "Sucesso; réplicas convergem depois", "node.py:gravar", "response"),
         ],
         ["Chave inválida gera erro gRPC; nó que não é líder retorna NOT_LEADER.",
          "Sem atividade de quórum, rejeita antes de acrescentar a entrada.",
          "Se expirar o prazo após append ou mudar a liderança, o resultado é indeterminado: não significa sucesso nem rollback garantido."]),
    criar_fluxo("consulta", "4. Consulta: ler em qualquer nó",
         "A leitura usa apenas o banco final do nó escolhido. Não busca o valor no líder.",
         "get 3 telefone consulta só o nó 3. getall telefone repete a leitura em cada nó.",
         ["Cliente", "Nó 3", "Banco final do nó 3"],
         [
             criar_etapa(0, 1, "Read: qual é o telefone?", "O cliente escolhe o nó. Ele pode ser seguidor, candidato ou líder.", "Consulta local", "node.py:consultar"),
             criar_etapa(1, 2, "SELECT na tabela kv", "A tabela kv contém somente o resultado das operações até o commit local. O log pendente não é consultado para responder.", "Somente dados comitados", "storage.py:consultar", "local"),
             criar_etapa(2, 1, "Retorna valor ou ausência", "Se ainda não conhece o novo commit, pode retornar o valor antigo ou não encontrar a chave. Nunca deve revelar uma entrada apenas pendente.", "Pode estar atrasado", "storage.py:consultar", "local"),
             criar_etapa(1, 0, "Resposta: found, value e commit", "O cliente recebe se a chave existe, o valor, o termo e o índice de commit local.", "Leitura eventual", "node.py:consultar", "response"),
         ],
         ["Não há votação nem quórum para ler; um nó acessível pode responder mesmo com outros desligados.",
          "Após novos Fetches e atualização do commit, o nó passa a mostrar a versão atual.",
          "Nó desligado não responde: o cliente informa falha de comunicação."]),
    criar_fluxo("redirecionamento", "5. Redirecionamento: pediu ao nó errado",
         "O seguidor informa quem é o líder; o cliente é quem refaz a chamada.",
         "put 3 telefone 9999, mas o líder conhecido é o nó 1.",
         ["Cliente", "Seguidor 3", "Líder 1"],
         [
             criar_etapa(0, 1, "Write para o nó 3", "Você pode começar a escrita por qualquer nó.", "Destino inicial escolhido", "client.py:gravar"),
             criar_etapa(1, 0, "NOT_LEADER; líder = 1", "O seguidor não acrescenta a operação ao log. Retorna o identificador do líder conhecido.", "Nenhuma escrita no seguidor", "node.py:gravar", "response"),
             criar_etapa(0, 2, "Cliente repete Write no nó 1", "O cliente segue o redirecionamento. O seguidor não encaminha internamente a operação.", "Entra no fluxo normal de gravação", "client.py:gravar"),
             criar_etapa(2, 0, "Resultado da escrita", "Se tudo ocorrer bem, só retorna sucesso após commit. Outros resultados, como NO_QUORUM, são mostrados ao usuário.", "Resultado recebido", "client.py:gravar", "response"),
         ],
         ["Se o líder ainda for desconhecido (0), o cliente devolve NOT_LEADER; aguarde a eleição.",
          "O cliente detecta ciclos de redirecionamento e informa erro.",
          "Não repete automaticamente escritas com resultado ambíguo por queda de conexão."]),
    criar_fluxo("queda", "6. Queda do líder: detectar e substituir",
         "O Fetch também é sinal de vida. Sem respostas, um seguidor acaba iniciando uma eleição.",
         "Nó 1 cai; nó 2 é eleito e o nó 3 passa a buscar nele.",
         ["Seguidor 2", "Líder 1 / caiu", "Nós 3 e 4"],
         [
             criar_etapa(0, 1, "Fetch tenta falar com líder antigo", "O pedido falha ou excede o prazo da chamada gRPC. A falha não renova o prazo de eleição.", "Sem resposta útil", "node.py:buscar_do_lider"),
             criar_etapa(0, 0, "Prazo de eleição expira", "Depois de tempo suficiente sem renovar seu prazo, o nó decide tentar uma eleição. Uma falha isolada de Fetch não dispara eleição imediatamente.", "Líder suspeito de falha", "node.py:executar", "local"),
             criar_etapa(0, 2, "RequestVote em termo maior", "O nó 2 vira candidato e pede votos. Com os nós 2, 3 e 4, ainda pode obter os três votos necessários.", "Eleição automática", "node.py:iniciar_eleicao"),
             criar_etapa(2, 0, "Votos permitem novo líder", "O candidato confirma a maioria e assume. O termo maior distingue esta rodada da liderança antiga.", "Nó 2 vira líder", "node.py:iniciar_eleicao", "response"),
             criar_etapa(2, 0, "Seguidores descobrem e buscam no novo", "Ao desconhecer o líder, consultam Status; termos maiores também são observados nas respostas. Depois voltam a enviar Fetch.", "Replicação retomada", "node.py:descobrir_lider"),
         ],
         ["O desenho mostra uma eleição bem-sucedida; podem ser necessárias outras rodadas.",
          "Com menos de três nós comunicando-se, não se elege um novo líder.",
          "O líder antigo, ao voltar, inicia como seguidor e precisa reconciliar seu histórico."]),
    criar_fluxo("quorum", "7. Perda de quórum: bloquear escrita",
         "O líder acompanha cada seguidor separadamente. Com menos de três ativos, não admite nova escrita.",
         "Nós 3 e 4 caíram. Restam líder 1 e seguidor 2: apenas dois.",
         ["Cliente", "Líder 1", "Seguidores 3 e 4"],
         [
             criar_etapa(2, 2, "Seguidores deixam de enviar Fetch", "Desligados ou sem comunicação, os dois nós não renovam seu sinal de atividade.", "Ausência de pedidos", "node.py:obter_replicas_ativas", "local"),
             criar_etapa(1, 1, "Últimos Fetches passam de 700 ms", "O líder calcula o tempo desde o último Fetch individual. Ao ultrapassar o limite, considera cada réplica inativa.", "Só 2 de 4 ativos contando o líder", "node.py:obter_replicas_ativas", "local"),
             criar_etapa(0, 1, "Write: guardar novo telefone", "O cliente ainda pode alcançar o líder; isso não significa que haja nós suficientes para replicar.", "Tentativa de escrita", "node.py:gravar"),
             criar_etapa(1, 0, "NO_QUORUM; não acrescenta entrada", "A verificação de atividade ocorre antes do append. A nova operação é rejeitada e não recebe confirmação.", "Sem nova escrita", "node.py:gravar", "response"),
         ],
         ["Se a perda ocorrer depois de uma operação já ter sido admitida, ela pode aguardar e terminar com resultado indeterminado.",
          "Cópias já confirmadas em disco continuam sendo evidência de armazenamento; atividade e confirmação de índice são controles diferentes.",
          "Leituras de dados já comitados continuam possíveis nos nós acessíveis."]),
    criar_fluxo("atraso", "8. Réplica atrasada: recuperar o que faltou",
         "Um seguidor pode ficar desligado enquanto os outros três continuam gravando.",
         "Nó 4 saiu no índice 5. O grupo chegou ao 8. Ao voltar, ele busca 6, 7 e 8.",
         ["Nó 4", "Disco do nó 4", "Líder atual"],
         [
             criar_etapa(0, 1, "Reabre seu banco ao iniciar", "Recupera termo, voto, commit, log e dados. Sempre inicia como seguidor; não apaga seu estado para entrar no grupo.", "Histórico preservado até 5", "storage.py:__init__", "local"),
             criar_etapa(0, 2, "Status: quem é o líder?", "Consulta os pares e observa os termos. Se encontrar termo maior, atualiza o seu antes de prosseguir.", "Líder descoberto", "node.py:descobrir_lider"),
             criar_etapa(0, 2, "Fetch: tenho até 5", "Informa o ponto até onde seu log está armazenado, com o termo daquela entrada.", "Busca das entradas faltantes", "node.py:buscar_do_lider"),
             criar_etapa(2, 0, "Resposta: 6, 7 e 8 + commit", "Se o prefixo confere, devolve as entradas faltantes em lotes de até 32, além do commit do líder.", "Dados recebidos via Pull", "node.py:atender_busca", "response"),
             criar_etapa(0, 1, "Salva log e atualiza banco confirmado", "Aplica o commit recebido apenas até o fim do log já disponível. Caso faltem mais entradas, continua nos ciclos seguintes.", "Réplica volta a convergir", "node.py:buscar_do_lider", "local"),
         ],
         ["Se houver mais de 32 entradas faltantes, usa vários Fetches.",
          "Se o histórico local divergir, segue o fluxo de truncamento.",
          "Até atualizar, leituras locais podem estar antigas."]),
    criar_fluxo("truncamento", "9. Divergência: corrigir o final do log",
         "Só a cauda não comitada pode ser substituída. O prefixo confirmado é protegido.",
         "Líder antigo tem [A, B, X pendente]. Novo líder tem [A, B, Y].",
         ["Antigo líder / seguidor", "Líder atual", "Disco do antigo"],
         [
             criar_etapa(0, 1, "Fetch informa o ponto de X", "O nó reiniciado tenta sincronizar a partir de seu último índice e termo.", "Logs podem divergir", "node.py:buscar_do_lider"),
             criar_etapa(1, 0, "Ponto não confere; tente antes", "Se o índice está além do log do líder ou o termo não coincide, responde com retry_index menor.", "Recua o cursor, não apaga ainda", "node.py:atender_busca", "response"),
             criar_etapa(0, 1, "Novo Fetch no ponto anterior", "A réplica recua e tenta novamente até encontrar um prefixo comum. Na figura, é o trecho [A, B].", "Busca do prefixo comum", "node.py:buscar_do_lider"),
             criar_etapa(1, 0, "Prefixo confere; aqui está Y", "A resposta contém o trecho correto depois do ponto comum, junto com o commit atual.", "Cauda correta recebida", "node.py:atender_busca", "response"),
             criar_etapa(0, 2, "Remove X e persiste [A, B, Y]", "Antes de substituir, verifica que nenhuma entrada já comitada será alterada. Registra TRUNCAMENTO e salva a correção.", "Entrada não comitada descartada", "node.py:buscar_do_lider", "local"),
         ],
         ["X não deve ter sido mostrado ao usuário nas leituras, pois não estava comitado.",
          "Se a alteração atingir o prefixo comitado, o programa sinaliza erro explícito.",
          "Uma gravação que perdeu a resposta pode ter sido confirmada ou não: ausência de resposta não prova que será truncada."]),
    criar_fluxo("isolamento", "10. Isolamento: recebido não é confirmado",
         "A réplica pode possuir uma operação no log e ainda responder com o valor antigo.",
         "telefone era 8888. A nova entrada diz 9999, mas seu índice ainda está acima do commit.",
         ["Seguidor", "Líder", "Banco final local", "Cliente"],
         [
             criar_etapa(0, 1, "Fetch pede novidades", "O seguidor usa o fluxo normal de busca.", "Banco ainda mostra 8888", "node.py:buscar_do_lider"),
             criar_etapa(1, 0, "Nova entrada; commit ainda anterior", "O líder devolve a operação 9999 antes de confirmar seu índice. Receber dados não é autorização para torná-los visíveis.", "Log contém 9999 pendente", "node.py:atender_busca", "response"),
             criar_etapa(0, 2, "Persiste sem aplicar a entrada pendente", "Ao salvar, o banco final é reconstruído somente com o prefixo até commit_index. Por isso, continua com 8888.", "Log e banco têm funções diferentes", "storage.py:salvar", "local"),
             criar_etapa(3, 0, "Read: qual telefone?", "A leitura consulta o banco final, não a última entrada do log.", "Retorna 8888, não 9999", "node.py:consultar"),
             criar_etapa(0, 1, "Outro Fetch confirma o armazenamento", "O novo pedido informa o índice persistido. Quando houver três confirmações e a regra do termo permitir, o líder avança o commit.", "Pode formar quórum", "node.py:atender_busca"),
             criar_etapa(1, 0, "Resposta com commit atualizado", "Pode ser esta resposta ou uma resposta posterior, dependendo de quando o quórum foi alcançado.", "Autorização para aplicar 9999", "node.py:atender_busca", "response"),
             criar_etapa(0, 2, "Aplica commit no banco local", "Agora 9999 integra o prefixo confirmado. A próxima leitura poderá retornar o novo valor.", "Dado visível", "node.py:buscar_do_lider", "local"),
         ],
         ["O teste automatizado pausa o avanço de commit para tornar esse intervalo fácil de observar.",
          "No funcionamento normal, o intervalo pode ser muito curto.",
          "Sucesso no cliente não implica que todos os seguidores já receberam o commit atualizado."]),
    criar_fluxo("termo", "11. Termo maior: abandonar liderança antiga",
         "Uma rodada maior recebida no protocolo obriga o nó a atualizar o estado e virar seguidor.",
         "O nó acredita estar no termo 2; recebe uma mensagem do termo 3.",
         ["Outro nó", "Nó no termo antigo", "Disco local"],
         [
             criar_etapa(0, 1, "Requisição ou resposta com termo 3", "Pode ocorrer em RequestVote, Fetch ou descoberta por Status. O termo acompanha as mensagens do protocolo.", "Termo recebido maior que 2", "node.py:observar_termo"),
             criar_etapa(1, 2, "Salva termo 3 e limpa voto anterior", "O voto anterior valia para outra rodada. O novo termo e a ausência de voto são persistidos.", "Novo termo durável", "node.py:observar_termo", "local"),
             criar_etapa(1, 1, "Passa a seguidor", "Mesmo se era líder ou candidato, abandona esse papel, limpa o líder conhecido e os controles de replicação da liderança antiga.", "Não aceita escrita como líder", "node.py:observar_termo", "local"),
             criar_etapa(1, 1, "Retoma descoberta e busca", "O loop volta ao comportamento de seguidor. Se não houver líder conhecido, consulta os pares; se o prazo expirar, pode iniciar outra eleição.", "Continuidade pelo protocolo normal", "node.py:executar", "local"),
         ],
         ["Termo igual ou menor não dispara essa atualização por si só.",
          "Uma escrita pendente pode deixar de receber confirmação ao perder a liderança."]),
    criar_fluxo("persistencia", "12. Persistência e reinício",
         "Os dados não vivem só na memória. Cada nó salva seu próprio estado em SQLite.",
         "Fechar o programa e abri-lo com o mesmo identificador e diretório mantém seu histórico.",
         ["Nó", "SQLite local", "Outros nós"],
         [
             criar_etapa(0, 1, "Salva estado em uma transação", "Atualiza metadata (termo, voto, commit), log (histórico) e kv (dados confirmados). O banco final ignora entradas noop e pendentes.", "Estado durável e consistente", "storage.py:salvar", "local"),
             criar_etapa(0, 0, "Processo é encerrado", "A memória do programa desaparece. O arquivo SQLite e seus auxiliares de persistência não devem ser apagados.", "Nó indisponível", "node.py:iniciar_servidor", "local"),
             criar_etapa(0, 1, "Reinicia e carrega o arquivo", "Lê termo, voto, índice de commit e log; o banco final já está salvo. Valida índices sequenciais e limites do commit.", "Estado recuperado", "storage.py:__init__", "local"),
             criar_etapa(0, 0, "Começa como seguidor", "O papel de líder não é restaurado automaticamente. Isso evita retornar assumindo uma liderança que pode ter sido substituída.", "Papel inicial seguro", "node.py:__init__", "local"),
             criar_etapa(0, 2, "Descobre líder e volta ao Pull", "Busca entradas faltantes e reconcilia divergências pelo mesmo protocolo dos demais seguidores.", "Reintegração", "node.py:descobrir_lider"),
         ],
         ["Persistir não significa comitar: uma entrada pendente também pode estar salva no disco.",
          "A implementação regrava o estado ao salvar, priorizando simplicidade didática em vez de desempenho para grandes volumes."]),
]

GLOSSARY = {
    "Nó": "Um programa que mantém sua própria cópia dos dados.",
    "Cluster": "O conjunto dos quatro nós.",
    "Cliente": "O programa no qual você pede gravações e consultas.",
    "Líder": "O nó eleito para organizar as gravações.",
    "Seguidor": "Um nó que busca o histórico do líder.",
    "Candidato": "Um nó pedindo votos para se tornar líder.",
    "Pull": "Estratégia: quem precisa dos dados toma a iniciativa de buscar.",
    "Fetch": "Cada pedido concreto de busca enviado pelo seguidor.",
    "Push": "Envio iniciado espontaneamente pelo líder. Não usado na replicação deste projeto.",
    "Log": "Lista ordenada de operações; pode conter operações ainda pendentes.",
    "Índice": "Posição de uma entrada na lista do log.",
    "Commit": "Confirmação que permite aplicar uma operação ao banco final.",
    "Índice de commit": "Até qual posição o histórico está confirmado neste nó.",
    "Quórum": "Maioria necessária: três dos quatro nós.",
    "Termo": "Número de uma rodada de eleição; não é o índice da operação.",
    "Heartbeat": "Sinal de atividade. Aqui, o Fetch e sua resposta cumprem esse papel.",
    "Timeout": "Prazo máximo de espera antes de agir ou informar falha.",
    "Persistência": "Salvar em disco para recuperar após reiniciar.",
    "Truncamento": "Remover uma cauda divergente não comitada do histórico.",
    "Noop": "Entrada interna do líder que não modifica chaves.",
    "gRPC": "Tecnologia das chamadas e respostas entre os programas.",
    "Consistência eventual": "Uma réplica pode estar atrasada; converge com a replicação funcionando.",
}

PAGE = r"""<!doctype html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Fluxos do cluster — Pull, eleição e gRPC</title>
<style>
:root{color-scheme:dark;--bg:#0d1117;--panel:#161b22;--soft:#1c2230;--border:#30363d;--text:#e6edf3;--muted:#a9b5c5;--blue:#58a6ff;--orange:#ffa657;--green:#63dba4}
@media(prefers-color-scheme:light){:root{color-scheme:light;--bg:#f6f8fa;--panel:#fff;--soft:#edf2f8;--border:#c8d1dc;--text:#1f2328;--muted:#526171;--blue:#0969da;--orange:#9a4600;--green:#117747}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:16px/1.6 system-ui,"Segoe UI",sans-serif}
header,main,footer{max-width:1280px;margin:auto;padding:24px}h1{font-size:clamp(1.7rem,4vw,2.5rem);line-height:1.2}h2{margin-top:0;font-size:1.3rem}h3{margin-bottom:6px}p{max-width:90ch}.muted,footer{color:var(--muted)}
.card{background:var(--panel);border:1px solid var(--border);border-radius:14px;padding:24px;margin-bottom:24px}
.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(230px,1fr));gap:12px}.term,.box{background:var(--soft);border:1px solid var(--border);border-radius:10px;padding:14px}.term b{display:block;color:var(--blue)}
nav{display:flex;flex-wrap:wrap;gap:8px}a{color:var(--blue)}nav a,button{border:1px solid var(--border);border-radius:8px;padding:8px 12px;background:var(--soft);color:var(--text);font:inherit;text-decoration:none;cursor:pointer}
button:hover,nav a:hover{border-color:var(--blue)}button:focus-visible,a:focus-visible,summary:focus-visible{outline:3px solid var(--blue);outline-offset:3px}
.example{border-left:4px solid var(--green);padding:10px 16px;background:var(--soft)}.legend{display:flex;flex-wrap:wrap;gap:18px;font-size:.88rem;margin:16px 0}.request{color:var(--blue)}.response{color:var(--muted)}.local{color:var(--orange)}
.diagram-wrap,.table-wrap{overflow-x:auto;border:1px solid var(--border);border-radius:10px;margin:16px 0}.diagram{display:block;width:100%;height:auto;min-width:880px;background:var(--bg)}svg text{font-family:inherit;fill:var(--text)}.lane{stroke:var(--border);stroke-dasharray:5 6}.actor{fill:var(--soft);stroke:var(--border)}.actor-label{font-size:14px;font-weight:700;text-anchor:middle}
.message{cursor:pointer;outline:none}.hit{fill:transparent}.message:hover .hit,.message:focus .hit,.message.active .hit{fill:var(--soft);stroke:var(--blue)}.message .caption{font-size:13px;text-anchor:middle;paint-order:stroke;stroke:var(--bg);stroke-width:5px;stroke-linejoin:round}.message .number{font-size:11px;fill:var(--bg);text-anchor:middle;font-weight:800}
table{width:100%;border-collapse:collapse;min-width:750px}th,td{text-align:left;padding:12px;border-bottom:1px solid var(--border);vertical-align:top}th{color:var(--muted);font-size:.85rem}td:first-child{width:60px}.state{display:block;color:var(--green);font-size:.85rem;margin-top:8px}
summary{cursor:pointer;color:var(--blue);padding:8px 0}li{margin:8px 0}code{font-family:Consolas,monospace;background:var(--soft);padding:2px 5px;border-radius:4px}
dialog{width:min(1000px,95vw);max-height:90vh;padding:0;border:1px solid var(--border);border-radius:14px;color:var(--text);background:var(--panel)}dialog::backdrop{background:#000a}dialog[open]{display:flex;flex-direction:column}.modal-head,.modal-foot{display:flex;justify-content:space-between;align-items:center;gap:12px;padding:18px;border-bottom:1px solid var(--border)}.modal-body{padding:0 20px 20px;overflow:auto}.modal-foot{border-top:1px solid var(--border);border-bottom:0}.modal-head h2{margin:0}.modal-foot button:disabled{opacity:.4;cursor:default}
.code{overflow:auto;background:var(--bg);border:1px solid var(--border);border-radius:8px;font:13px/1.65 Consolas,monospace;padding:12px 0;max-height:44vh}.code-line{display:flex;min-width:max-content}.line-number{width:55px;flex-shrink:0;text-align:right;padding-right:16px;color:var(--muted);user-select:none}.source-line{white-space:pre;padding-right:16px}.file{color:var(--muted);font-size:.9rem;overflow-wrap:anywhere}
@media(max-width:600px){header,main,footer{padding:16px}.card{padding:16px}.modal-head,.modal-foot{padding:12px}.keyboard{display:none}}
@media print{body{background:white;color:black}.card{break-inside:avoid}nav,.step-button,dialog{display:none}.diagram-wrap{overflow:visible}.diagram{min-width:0}details>*{display:block}}
</style>
</head>
<body>
<header>
<p class="muted">Guia visual do trabalho de Sistemas Distribuídos</p>
<h1>O que acontece em cada evento do cluster?</h1>
<p>Leia as setas <strong>de cima para baixo</strong>. Clique em uma etapa para ver a explicação e o <strong>código real</strong> que a implementa. Tudo funciona offline, sem iniciar o cluster.</p>
<div class="grid">
<div class="box"><strong>Quatro nós</strong><br>Quatro programas com discos separados; qualquer um pode ser eleito líder.</div>
<div class="box"><strong>Um cliente</strong><br>Você pede gravações e consulta o nó que escolher.</div>
<div class="box"><strong>Regra de segurança</strong><br>Uma escrita precisa ser confirmada em 3 de 4 nós para receber sucesso.</div>
</div>
</header>
<main>
<section class="card"><h2>Escolha um evento</h2><nav id="navigation" aria-label="Eventos do cluster"></nav></section>
<section class="card"><h2>Antes dos fluxos: três coisas diferentes</h2>
<div class="grid"><div class="box"><strong>Pull = estratégia</strong><br>O seguidor toma a iniciativa de buscar novidades.</div><div class="box"><strong>Fetch = pedido</strong><br>“Tenho até a entrada 4. Tem algo novo?” Cada busca é um Fetch.</div><div class="box"><strong>Push = outra estratégia</strong><br>O líder enviaria sem esperar um pedido. Não é usado aqui. Responder a um Fetch não é Push.</div></div>
<p><strong>Atenção:</strong> “salvo no log” não significa “confirmado”. A consulta lê somente o banco final comitado.</p>
</section>
<div id="flows"></div>
<section class="card" id="glossario"><h2>Dicionário rápido</h2><div class="grid" id="terms"></div></section>
</main>
<footer>Diagramas didáticos baseados no código deste projeto. Setas agrupadas representam chamadas independentes; não há banco compartilhado. Trechos são uma fotografia do código na geração: execute <code>python gerar_fluxos.py</code> após mudanças. Esta página não executa pedidos gRPC nem simula um cluster real.</footer>
<dialog id="detail" aria-labelledby="detail-title">
<div class="modal-head"><div><small id="detail-position" class="muted"></small><h2 id="detail-title"></h2></div><button id="close" aria-label="Fechar detalhes" type="button">Fechar ×</button></div>
<div class="modal-body"><p id="detail-route" class="muted"></p><p id="detail-explanation"></p><p id="detail-state" class="state"></p><h3>Código que participa desta etapa</h3><p class="file" id="detail-file"></p><div class="code" id="detail-code" tabindex="0" role="region" aria-label="Trecho de código-fonte"></div><p class="muted">O trecho mostra a função real completa. Algumas etapas compartilham a mesma função porque são partes de uma única operação.</p></div>
<div class="modal-foot"><button id="previous" type="button">← Anterior</button><span class="keyboard">← → navegam · Esc fecha</span><button id="next" type="button">Próxima →</button></div>
</dialog>
<script id="flow-data" type="application/json">__DATA__</script>
<script>
"use strict";
const data=JSON.parse(document.getElementById("flow-data").textContent);
const NS="http://www.w3.org/2000/svg";
function criar_elemento(tag,attrs={},text){const n=document.createElement(tag);for(const [k,v] of Object.entries(attrs))n.setAttribute(k,v);if(text!==undefined)n.textContent=text;return n}
function criar_elemento_svg(tag,attrs={},text){const n=document.createElementNS(NS,tag);for(const [k,v] of Object.entries(attrs))n.setAttribute(k,v);if(text!==undefined)n.textContent=text;return n}
const dialog=document.getElementById("detail");let currentFlow=0,currentStep=0,opener=null;
function abrir_etapa(fi,si,trigger){
 const f=data.flows[fi],s=f.steps[si],c=data.code[s.symbol];currentFlow=fi;currentStep=si;
 if(!dialog.open)opener=trigger;
 document.querySelectorAll(".message.active").forEach(n=>n.classList.remove("active"));
 document.getElementById(`${f.id}-step-${si}`).classList.add("active");
 document.getElementById("detail-position").textContent=`${f.title} · Etapa ${si+1} de ${f.steps.length}`;
 document.getElementById("detail-title").textContent=s.label;
 document.getElementById("detail-route").textContent=`${f.actors[s.source]} → ${f.actors[s.target]} · ${s.kind==="request"?"Pedido gRPC":s.kind==="response"?"Resposta gRPC":"Ação local (sem rede)"}`;
 document.getElementById("detail-explanation").textContent=s.explanation;
 document.getElementById("detail-state").textContent=`Estado: ${s.state}`;
 document.getElementById("detail-file").textContent=`${c.file} · ${c.symbol} · linhas ${c.start}–${c.end}`;
 const code=document.getElementById("detail-code");code.replaceChildren();
 c.source.split("\n").forEach((line,i)=>{const row=criar_elemento("div",{class:"code-line"});row.append(criar_elemento("span",{class:"line-number","aria-hidden":"true"},c.start+i),criar_elemento("span",{class:"source-line"},line));code.append(row)});
 code.scrollTop=0;code.scrollLeft=0;
 document.getElementById("previous").disabled=si===0;document.getElementById("next").disabled=si===f.steps.length-1;
 if(!dialog.open)dialog.showModal();
}
data.flows.forEach((f,fi)=>{
 document.getElementById("navigation").append(criar_elemento("a",{href:`#${f.id}`},f.title));
 const card=criar_elemento("section",{class:"card",id:f.id,"aria-labelledby":`${f.id}-title`});
 card.append(criar_elemento("h2",{id:`${f.id}-title`},f.title),criar_elemento("p",{},f.summary),criar_elemento("p",{class:"example"},`Exemplo: ${f.example}`));
 const legend=criar_elemento("div",{class:"legend"});
 for(const [kind,text] of [["request","━━ Pedido gRPC"],["response","┄┄ Resposta gRPC"],["local","━━ Ação local / disco"]])legend.append(criar_elemento("span",{class:kind},text));
 card.append(legend,criar_elemento("p",{class:"muted"},"Clique nas setas ou nos botões da tabela para abrir a explicação e o código. A ordem abaixo representa um exemplo possível."));
 const width=1100,height=150+f.steps.length*82, xs=f.actors.map((_,i)=>95+i*(width-300)/(f.actors.length-1));
 const canvas=criar_elemento_svg("svg",{class:"diagram",viewBox:`0 0 ${width} ${height}`,role:"group","aria-label":`Diagrama: ${f.title}`});
 const defs=criar_elemento_svg("defs");
 for(const [kind,color] of [["request","var(--blue)"],["response","var(--muted)"],["local","var(--orange)"]]){
  const marker=criar_elemento_svg("marker",{id:`${f.id}-${kind}`,viewBox:"0 0 10 10",refX:9,refY:5,markerWidth:7,markerHeight:7,orient:"auto-start-reverse"});
  marker.append(criar_elemento_svg("path",{d:"M0 0 L10 5 L0 10 Z",fill:color}));defs.append(marker)
 }
 canvas.append(defs);
 f.actors.forEach((name,i)=>{canvas.append(criar_elemento_svg("line",{class:"lane",x1:xs[i],x2:xs[i],y1:75,y2:height-25}),criar_elemento_svg("rect",{class:"actor",x:xs[i]-90,y:15,width:180,height:60,rx:9}));
 const label=criar_elemento_svg("text",{class:"actor-label",x:xs[i],y:40});name.split(" / ").forEach((part,j)=>label.append(criar_elemento_svg("tspan",{x:xs[i],dy:j?18:0},part)));canvas.append(label)});
 f.steps.forEach((s,si)=>{
  const x1=xs[s.source],x2=xs[s.target],y=125+si*82,local=s.source===s.target,color=s.kind==="request"?"var(--blue)":s.kind==="response"?"var(--muted)":"var(--orange)";
  const g=criar_elemento_svg("g",{id:`${f.id}-step-${si}`,class:"message",tabindex:0,role:"button","aria-label":`Etapa ${si+1}: ${s.label}. Abrir explicação e código.`});
  const left=local?x1-20:Math.min(x1,x2)-18,span=local?285:Math.abs(x2-x1)+36;
  g.append(criar_elemento_svg("title",{},s.explanation),criar_elemento_svg("rect",{class:"hit",x:left,y:y-33,width:span,height:64,rx:8}));
  const attrs={fill:"none",stroke:color,"stroke-width":2,"marker-end":`url(#${f.id}-${s.kind})`};
  if(s.kind==="response")attrs["stroke-dasharray"]="6 5";
  if(local)g.append(criar_elemento_svg("path",{...attrs,d:`M${x1} ${y} h55 v20 h-55`}));
  else g.append(criar_elemento_svg("line",{...attrs,x1,y1:y,x2,y2:y}));
  g.append(criar_elemento_svg("circle",{cx:x1,cy:y,r:12,fill:color}),criar_elemento_svg("text",{class:"number",x:x1,y:y+4},si+1));
  const label=criar_elemento_svg("text",{class:"caption",x:local?x1+120:(x1+x2)/2,y:y-13},s.label);g.append(label);
  g.addEventListener("click",()=>abrir_etapa(fi,si,g));g.addEventListener("keydown",e=>{if(e.key==="Enter"||e.key===" "){e.preventDefault();abrir_etapa(fi,si,g)}});canvas.append(g)
 });
 const wrap=criar_elemento("div",{class:"diagram-wrap"});wrap.append(canvas);card.append(wrap);
 const tableWrap=criar_elemento("div",{class:"table-wrap"}),table=criar_elemento("table"),head=criar_elemento("thead"),hr=criar_elemento("tr");
 ["#","Etapa","Em palavras simples","Detalhes"].forEach(t=>hr.append(criar_elemento("th",{scope:"col"},t)));head.append(hr);table.append(head);
 const body=criar_elemento("tbody");f.steps.forEach((s,si)=>{const row=criar_elemento("tr"),description=criar_elemento("td",{},s.explanation);description.append(criar_elemento("span",{class:"state"},s.state));const cell=criar_elemento("td"),button=criar_elemento("button",{type:"button",class:"step-button","aria-label":`Ver código da etapa ${si+1} de ${f.title}`},"Ver código");button.addEventListener("click",()=>abrir_etapa(fi,si,button));cell.append(button);row.append(criar_elemento("td",{},si+1),criar_elemento("td",{},s.label),description,cell);body.append(row)});table.append(body);tableWrap.append(table);card.append(tableWrap);
 const details=criar_elemento("details"),list=criar_elemento("ul");details.append(criar_elemento("summary",{},"E se algo der errado? Observações importantes"));f.alternatives.forEach(t=>list.append(criar_elemento("li",{},t)));details.append(list);card.append(details);
 document.getElementById("flows").append(card)
});
document.getElementById("navigation").append(criar_elemento("a",{href:"#glossario"},"Dicionário"));
Object.entries(data.glossary).forEach(([term,meaning])=>{const box=criar_elemento("div",{class:"term"});box.append(criar_elemento("b",{},term),criar_elemento("span",{},meaning));document.getElementById("terms").append(box)});
document.getElementById("close").addEventListener("click",()=>dialog.close());
document.getElementById("previous").addEventListener("click",()=>abrir_etapa(currentFlow,currentStep-1));
document.getElementById("next").addEventListener("click",()=>abrir_etapa(currentFlow,currentStep+1));
dialog.addEventListener("keydown",e=>{if(e.target.closest(".code"))return;if(e.key==="ArrowRight"&&currentStep<data.flows[currentFlow].steps.length-1){e.preventDefault();abrir_etapa(currentFlow,currentStep+1)}if(e.key==="ArrowLeft"&&currentStep>0){e.preventDefault();abrir_etapa(currentFlow,currentStep-1)}});
dialog.addEventListener("close",()=>{document.querySelectorAll(".message.active").forEach(n=>n.classList.remove("active"));if(opener)opener.focus({preventScroll:true})});
</script>
</body>
</html>
"""


def extrair_codigo():
    # A arvore sintatica encontra a funcao mesmo quando comentarios mudam as linhas.
    code = {}
    for symbol in {s["symbol"] for f in FLOWS for s in f["steps"]}:
        filename, name = symbol.split(":")
        text = (ROOT / filename).read_text(encoding="utf-8")
        tree = ast.parse(text)
        matches = [node for node in ast.walk(tree)
                   if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
                   and node.name == name]
        if len(matches) != 1:
            raise ValueError(f"Simbolo ausente ou ambiguo: {symbol}")
        node = matches[0]
        code[symbol] = dict(file=filename, symbol=name, start=node.lineno,
                            end=node.end_lineno,
                            source="\n".join(text.splitlines()[node.lineno - 1:node.end_lineno]))
    return code


def principal():
    # Escapa "<" para que um trecho de codigo nao encerre o bloco JSON do HTML.
    payload = json.dumps(dict(flows=FLOWS, glossary=GLOSSARY, code=extrair_codigo()),
                         ensure_ascii=False).replace("<", "\\u003c")
    destination = ROOT / "fluxos_cluster.html"
    destination.write_text(PAGE.replace("__DATA__", payload), encoding="utf-8")
    print(f"Gerado: {destination}")
    print(f"{len(FLOWS)} fluxos; {sum(len(f['steps']) for f in FLOWS)} etapas.")


if __name__ == "__main__":
    principal()
