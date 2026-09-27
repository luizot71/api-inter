# Guia prático para a entrevista de 28/09/2026

Você já conhece Java, Spring Boot, Docker e Kafka. O objetivo de hoje, 27/09, é ganhar autonomia em Micronaut e explicar as decisões desta API. O PDF pede o ambiente e os dois Testcontainers; não informa domínio, duração ou perguntas da entrevista. Esta API de notas é um laboratório autoral, não o enunciado oficial.

## 1. Execute antes de estudar

Abra PowerShell na pasta do projeto e mantenha Docker Desktop aberto:

```powershell
Set-Location 'C:\Users\luizo\OneDrive\Área de Trabalho\api inter'
mise exec -- mvn clean verify
```

Os testes iniciam containers próprios, usam portas dinâmicas e não dependem do Compose. Não use `-DskipTests` para demonstrar prontidão.

Para rodar a aplicação e fazer requisições manualmente:

```powershell
docker compose up -d
docker compose logs --tail 20
mise exec -- mvn mn:run
```

Deixe esse terminal aberto. Em outro PowerShell na mesma pasta:

```powershell
./scripts/demo.ps1
```

Se a política de scripts do computador bloquear o arquivo, abra `scripts/demo.ps1` na IDE e execute suas linhas manualmente. Não precisa alterar a política global.

O Compose usa PostgreSQL na porta 55432 e Kafka na 59092, vinculadas ao localhost. As credenciais `study/study` são apenas para este laboratório. Não use esse arquivo como configuração de produção. O Hibernate usa `update` neste laboratório; em produção, use migrações versionadas e validação do schema. Os dados do Compose não têm persistência garantida após remover os containers.

Para encerrar: Ctrl+C no terminal da aplicação e `docker compose down` na pasta deste projeto. Isso não encerra os outros projetos Docker.

## 2. Contrato da API

| Operação | Comportamento |
|---|---|
| POST /notes com UUID novo e conteúdo válido | 201, corpo com id/content e Location |
| GET /notes/{id} existente | 200 e nota persistida |
| GET com UUID válido inexistente | 404 |
| POST sem id, conteúdo vazio/branco ou maior que 255 | 400 |
| POST com mesmo id e mesmo conteúdo | 200, mesma nota, nova tentativa de publicação |
| POST com mesmo id e conteúdo diferente | 409, conteúdo original preservado |
| Publicação sem confirmação após commit | 503; nota permanece no banco; repetir mesmo id/conteúdo |

O cliente escolhe um UUID e o mantém em todas as tentativas da mesma operação. Criar outro UUID a cada retry constitui outra operação e pode criar outra nota. A comparação do conteúdo é exata, sem trim ou normalização. Não há alteração/exclusão pública da nota neste exercício.

Teste manual no PowerShell:

```powershell
$id = [guid]::NewGuid().ToString()
$body = @{ id = $id; content = 'Estudar Micronaut' } | ConvertTo-Json
Invoke-WebRequest -Method Post -Uri 'http://localhost:8080/notes' -ContentType 'application/json' -Body $body
Invoke-RestMethod -Uri "http://localhost:8080/notes/$id"
# Repetição idêntica: 200.
Invoke-WebRequest -Method Post -Uri 'http://localhost:8080/notes' -ContentType 'application/json' -Body $body
# Conflito: PowerShell exibirá a resposta de erro 409.
$conflict = @{ id = $id; content = 'Outro conteudo' } | ConvertTo-Json
Invoke-WebRequest -Method Post -Uri 'http://localhost:8080/notes' -ContentType 'application/json' -Body $conflict
```

Verifique o efeito do consumidor no banco (aguarde alguns segundos se necessário):

```powershell
docker compose exec postgres psql -U study -d inter_study -c 'SELECT * FROM preparation_note;'
docker compose exec postgres psql -U study -d inter_study -c 'SELECT * FROM note_receipt;'
```

## 3. Entenda o fluxo seguindo os arquivos

1. `notes/CreateNoteRequest`: record de entrada. `@Serdeable` permite serialização; `@NotNull`, `@NotBlank` e `@Size` declaram restrições. O DTO não é a entidade.
2. `notes/NoteController`: mapeia POST/GET. `@Valid` dispara validação. `@ExecuteOn(BLOCKING)` tira JDBC e a espera pela publicação do event loop HTTP. A resposta distingue criação e repetição.
3. `notes/NoteService`: coordena persistência e publicação. A transação já terminou quando o produtor é chamado. O serviço aguarda confirmação por até dez segundos, sem segurar a transação do banco.
4. `notes/NoteStore`: controla sessão/transação Hibernate explicitamente, para ficar comparável ao teste que você já estudou. Usa SQL PostgreSQL `ON CONFLICT DO NOTHING` para inserção concorrente e JPA para leitura. Essa escolha vincula esse trecho ao PostgreSQL; não é uma implementação portável de repositório. Uma evolução seria usar Micronaut Data e uma fronteira transacional declarativa.
5. `PreparationNote`: entidade persistida, com UUID como chave primária.
6. `notes/NoteProducer`: interface `@KafkaClient`. Micronaut gera a implementação; `@Topic` define o tópico e `@KafkaKey` define a chave. O ID é a chave; conteúdo é uma string. Um evento de produção poderia ter eventId, versão, instante e metadados.
7. `notes/NoteConsumer`: listener Micronaut consome o evento, grava `NoteReceipt` com chave única e confirma o banco antes de retornar. `SYNC_PER_RECORD` confirma o offset após processamento. Há três retries; esgotados, a partição é pausada, exigindo investigação/retomada operacional.

O efeito demonstrado pelo consumidor é somente gravar um recibo. Repetições do mesmo ID não criam recibos extras. Mensagens externas com mesmo ID e conteúdo divergente seriam ignoradas por esse consumidor; a API impede esse caso para suas próprias publicações, mas um contrato de eventos mais amplo exigiria validação adicional.

Não diga que esta API tem entrega exactly-once. Ela admite publicação duplicada e torna o efeito demonstrado idempotente por uma chave persistente.

## 4. Explique os testes existentes em dois minutos

### PostgreSQL: ApiInterTest

`TestPropertyProvider.getProperties()` inicia `postgres:16-alpine` antes da construção do contexto Micronaut. `getJdbcUrl()`, `getUsername()` e `getPassword()` retornam a conexão real do container; não fixamos porta 5432. O DataSource/Hikari usa essas propriedades.

`@MicronautTest(transactional = false)` impede que uma transação automática do teste esconda o limite real de commit. O teste abre uma sessão, inicia transação, grava e faz commit; depois fecha a sessão. A segunda sessão não tem o cache de primeiro nível da primeira. Os caches de segundo nível e de consulta estão desabilitados no YAML de teste. Assim a leitura verifica o PostgreSQL.

Flush envia SQL, mas não equivale a commit. Outra transação normalmente não verá alterações ainda não confirmadas. O teste também remove o registro criado. O container tem escopo da JVM e é removido pelo Ryuk ao terminar.

### Kafka: KafkaIntegrationTest

JUnit/Testcontainers inicia o broker via `@Container`. `getBootstrapServers()` fornece o endereço dinâmico. O tópico e grupo são exclusivos; o produtor espera confirmação, e o consumidor verifica chave e payload usando Awaitility. `pollInSameThread()` evita uso do KafkaConsumer por threads diferentes. Há timeout; não há uma espera fixa presumindo que a mensagem chegou. A extensão encerra o container.

### Fluxo completo: NotesHttpTest

Inicia os dois serviços antes do Micronaut, cria o tópico e injeta as propriedades. Envia HTTP real ao servidor embutido, lê a nota por GET e aguarda um recibo no PostgreSQL, escrito pelo listener Micronaut. Isso valida controller, DTO, persistência, produtor e consumidor juntos.

O teste de falha usa um produtor falso que retorna falha de forma determinística, com PostgreSQL real. Não simula todos os comportamentos de uma indisponibilidade de rede. O retry posterior usa HTTP e Kafka reais. O teste concorrente dispara duas chamadas com mesmo ID e espera um 201 e um 200, e só uma linha no banco.

```powershell
mise exec -- mvn '-Dtest=NotesHttpTest' test
mise exec -- mvn '-Dtest=NotesHttpTest#concurrentRequestsCreateOnlyOneNote' test
mise exec -- mvn '-Dtest=NotesHttpTest#publicationFailureKeepsCommitAndIdenticalRetryRecovers' test
```

## 5. Mudança de requisito: impedir duplicidade

Enunciado de treino: "O cliente pode repetir uma requisição após timeout. Não podemos cadastrar a nota duas vezes."

Antes de codificar, pergunte: como identificar a mesma operação? O que ocorre se o conteúdo mudar? Por quanto tempo a chave vale? Qual resposta será repetida?

Nesta solução: UUID é a identidade, mesmo payload retorna 200, payload diferente retorna 409. A PK é a defesa final contra concorrência. Fazer somente `find` e depois `insert` permite que duas requisições leiam ausência ao mesmo tempo. O SQL atômico elimina essa janela para criação.

Mesmo um retry HTTP idempotente pode republicar o evento. Por isso o consumidor também deduplica no banco; não basta uma coleção em memória, que se perde no restart e não é compartilhada entre instâncias.

## 6. Falhas entre banco e Kafka: o que responder

| Ponto de falha | Consequência nesta implementação |
|---|---|
| Antes do commit | Rollback; não publica |
| Depois do commit, antes da publicação | Nota existe, evento pode faltar; retry do cliente recupera |
| Kafka recebeu, confirmação não chegou | API pode responder 503 apesar do envio; retry pode duplicar mensagem |
| Consumidor gravou e caiu antes de confirmar offset | Mensagem pode repetir; PK do recibo evita efeito duplicado |
| Banco do consumidor indisponível por todos os retries | Partição pausada; precisa intervenção, sem recuperação automática implementada |

**Limitação central:** se o processo cair depois de gravar e o cliente nunca repetir, não existe rotina que republique automaticamente. `@Transactional` no banco não torna Kafka e PostgreSQL uma transação única. Transações Kafka não incluem automaticamente o banco.

**Evolução com outbox (discussão, não implementada):** gravar nota e evento pendente na mesma transação PostgreSQL; um publicador envia pendências e marca envio após confirmação. Se ele cair após enviar e antes de marcar, pode enviar de novo: consumidor idempotente continua necessário. Acrescente retries, observabilidade, tratamento de eventos inválidos e estratégia de retenção. Não implemente tudo na véspera sem dominar o fluxo atual.

Simulação manual opcional: com aplicação em execução, `docker compose stop kafka`, envie um POST com um UUID salvo e observe 503; faça GET e confira a nota. Depois `docker compose start kafka`, espere o broker ficar disponível e repita o POST com o mesmo corpo. Verifique o recibo. Essa falha pode levar alguns segundos por causa dos timeouts.

## 7. Plano de hoje (27/09): quatro horas focadas

| Tempo | Atividade | Entrega pessoal |
|---|---|---|
| 0:00–0:30 | Executar testes e demo | Explicar 201, 200, 400, 404, 409 e 503 |
| 0:30–1:15 | Ler DTO → controller → store | Desenhar fluxo e limite da transação sem consultar |
| 1:15–2:00 | Ler producer → consumer e testes | Explicar confirmação do envio, offset e duplicidade |
| 2:00–2:15 | Pausa | Descansar |
| 2:15–3:15 | Simulado abaixo | Implementar mudança pequena com teste |
| 3:15–4:00 | Revisar falhas, README e apresentação | Defender escolhas e reconhecer limitações |

Se só houver duas horas, mantenha execução, leitura do fluxo e 30 minutos de simulado. Não gaste esse período revisando fundamentos de Java/Docker/Kafka que você já domina.

## 8. Simulado de 60 minutos

Sem consultar uma solução pronta: "Agora toda nota deve conter um título obrigatório de até 80 caracteres, que também precisa aparecer no GET. Repetição com mesmo ID e título diferente deve gerar conflito."

- 0–10 min: confirmar contrato, compatibilidade de dados existentes e critérios de aceite.
- 10–35 min: alterar DTO, entidade, persistência e resposta; considerar o SQL explícito.
- 35–50 min: testar criação válida, título vazio, conflito e GET.
- 50–60 min: executar suíte, revisar diff e explicar impacto no evento.

Faça esse exercício em uma branch de estudo (`git switch -c estudo/titulo-nota`) depois de salvar a base. Não descarte alterações não salvas para voltar. Não precisa terminar tudo: comunique o que está funcionando e o que falta.

Critérios pessoais: consigo executar sem ajuda? Sei por que existe cada classe? Consigo localizar um erro pela causa? Tenho teste de caso negativo? Consigo explicar uma limitação sem inventar garantia?

## 9. Apresentação de três minutos

"Preparei um serviço Micronaut com Java 25 e testes em PostgreSQL/Kafka reais via Testcontainers. A API cria e consulta notas, valida DTOs e trata repetição de operação com UUID e restrição no banco. Após commit, publica pelo cliente Kafka Micronaut; um listener grava um recibo idempotente. Testei HTTP, persistência, consumo, concorrência e falha de publicação. A principal limitação é a janela entre banco e Kafka; para recuperação independente do cliente, adotaria outbox."

Fale com suas palavras. Se não conseguir explicar uma linha, investigue-a antes de acrescentar funcionalidades.

## 10. Amanhã, antes da chamada

Reserve 30–45 minutos: iniciar Docker, abrir IDE com Java 25, rodar testes uma vez, abrir README e verificar o link do GitHub. Não atualizar dependências nem renomear pacotes na última hora. Confirme com o entrevistador as regras de consulta e uso de ferramentas.

Perguntas para treinar: por que DTO separado? Por que executor blocking? Por que segunda sessão? Como evitar corrida na criação? A resposta 503 garante que nada foi gravado? O consumidor pode receber duas vezes? O que outbox resolve e o que não resolve?

## Fontes oficiais consultadas

- https://micronaut-projects.github.io/micronaut-kafka/latest/guide/ — KafkaClient, KafkaListener, offsets e retries.
- https://java.testcontainers.org/modules/kafka/ — container Apache Kafka.

Os comportamentos da implementação devem ser verificados pelos testes locais. O formato exato da entrevista não foi informado no PDF.
