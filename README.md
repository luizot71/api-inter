## Micronaut 5.1.5 Documentation

- [User Guide](https://docs.micronaut.io/5.1.5/guide/index.html)
- [API Reference](https://docs.micronaut.io/5.1.5/api/index.html)
- [Configuration Reference](https://docs.micronaut.io/5.1.5/guide/configurationreference.html)
- [Micronaut Guides](https://guides.micronaut.io/index.html)
---

- [Micronaut Maven Plugin documentation](https://micronaut-projects.github.io/micronaut-maven-plugin/latest/)
## Feature problem-json documentation


- [Micronaut Problem JSON documentation](https://micronaut-projects.github.io/micronaut-problem-json/latest/guide/index.html)


## Feature jdbc-hikari documentation


- [Micronaut Hikari JDBC Connection Pool documentation](https://micronaut-projects.github.io/micronaut-sql/latest/guide/index.html#jdbc)


## Feature jakarta-data documentation


- [Micronaut Jakarta Data documentation](https://micronaut-projects.github.io/micronaut-data/latest/guide/#jakartaData)


- [https://jakarta.ee/specifications/data/1.0/jakarta-data-1.0](https://jakarta.ee/specifications/data/1.0/jakarta-data-1.0)


## Feature management documentation


- [Micronaut Management documentation](https://docs.micronaut.io/latest/guide/index.html#management)


## Feature testcontainers documentation


- [https://www.testcontainers.org/](https://www.testcontainers.org/)


## Feature assertj documentation


- [https://assertj.github.io/doc/](https://assertj.github.io/doc/)


## Feature micronaut-aot documentation


- [Micronaut AOT documentation](https://micronaut-projects.github.io/micronaut-aot/latest/guide/)


## Feature kafka documentation


- [Micronaut Kafka Messaging documentation](https://micronaut-projects.github.io/micronaut-kafka/latest/guide/index.html)


## Feature maven-enforcer-plugin documentation


- [https://maven.apache.org/enforcer/maven-enforcer-plugin/](https://maven.apache.org/enforcer/maven-enforcer-plugin/)


## Feature junit-params documentation


- [https://junit.org/junit5/docs/current/user-guide/#writing-tests-parameterized-tests](https://junit.org/junit5/docs/current/user-guide/#writing-tests-parameterized-tests)


## Feature validation documentation


- [Micronaut Validation documentation](https://micronaut-projects.github.io/micronaut-validation/latest/guide/)


## Feature hibernate-jpa documentation


- [Micronaut Hibernate JPA documentation](https://micronaut-projects.github.io/micronaut-sql/latest/guide/index.html#hibernate)


## Feature hibernate-jpamodelgen documentation


- [Micronaut Hibernate JPA Static Metamodel Generator documentation](https://micronaut-projects.github.io/micronaut-data/latest/guide/#typeSafeJava)


- [https://hibernate.org/orm/tooling/](https://hibernate.org/orm/tooling/)


## Feature lombok documentation


- [Micronaut Project Lombok documentation](https://docs.micronaut.io/latest/guide/index.html#lombok)


- [https://projectlombok.org/features/all](https://projectlombok.org/features/all)


## Feature serialization-jackson documentation


- [Micronaut Serialization Jackson Core documentation](https://micronaut-projects.github.io/micronaut-serialization/latest/guide/)


## Feature awaitility documentation


- [https://github.com/awaitility/awaitility](https://github.com/awaitility/awaitility)


## Feature jspecify documentation


- [Micronaut JSpecify Nullability Annotations documentation](https://docs.micronaut.io/latest/guide/#jspecify)


- [https://jspecify.dev/docs/start-here/](https://jspecify.dev/docs/start-here/)



## Testes reais de PostgreSQL e Kafka

Requisitos: Docker Desktop iniciado em modo Linux, Java 25 e Maven 3.9 pelo Mise.
As dependencias de Testcontainers, AssertJ e Awaitility ja estao no pom.xml.

No PowerShell, a partir da raiz do projeto:

```powershell
mise exec -- mvn clean verify
```

Executar somente Kafka:

```powershell
mise exec -- mvn '-Dtest=KafkaIntegrationTest' test
```

Executar somente a persistencia JPA:

```powershell
mise exec -- mvn '-Dtest=ApiInterTest#shouldPersistAndReadNoteInANewSession' test
```

As aspas preservam os argumentos no PowerShell e no Git Bash. No IntelliJ, use JDK 25 para o projeto e para o executor de testes. E possivel executar cada classe pelo icone ao lado dela.

### Arquivos e comportamento

- `src/test/java/co/inter/piggies/KafkaIntegrationTest.java`: JUnit inicia `apache/kafka-native:4.3.1` com `@Container`. O teste obtem o bootstrap server por `getBootstrapServers()`, cria topico e grupo exclusivos, aguarda confirmacao de envio e verifica topico, chave e conteudo recebidos. Awaitility limita a espera pelo recebimento a 30 segundos; o polling usa a mesma thread por causa do KafkaConsumer. Esse limite e da etapa de consumo, nao de todo o build ou download de imagens. Producer, consumer e admin sao fechados; o topico e removido; a extensao encerra o container.
- `src/test/java/co/inter/piggies/ApiInterTest.java`: inicia `postgres:16-alpine` antes do contexto Micronaut pelo `TestPropertyProvider`, fornecendo URL e credenciais dinamicas. Desabilita a transacao automatica de teste para controlar o commit. Grava PreparationNote, fecha a sessao e consulta em outra sessao/transacao. Confere ID e conteudo, e remove o registro. O container fica no escopo da JVM e sua limpeza e feita pelo Testcontainers/Ryuk ao encerrar o processo.
- `src/test/resources/application-test.yml`: usa driver PostgreSQL normal, pois o container e iniciado explicitamente. `create-drop` vale apenas para o ambiente de testes; caches de segundo nivel e de consultas ficam desativados.

Nao e preciso iniciar PostgreSQL ou Kafka manualmente nem fixar portas. O teste Kafka usa os clientes Apache diretamente: valida a integracao real com o broker, mas nao implementa produtor/listener anotado do Micronaut. Esse e um exercicio posterior.

A suite possui tres testes: inicializacao da aplicacao, persistencia JPA e envio/consumo Kafka. Os relatorios ficam em `target/surefire-reports`. Se o Docker estiver indisponivel, os testes devem falhar, e nao serem silenciosamente ignorados.

### Versionar depois de validar

```bash
git add src/test/java/co/inter/piggies/ApiInterTest.java src/test/java/co/inter/piggies/KafkaIntegrationTest.java src/test/resources/application-test.yml README.md
git diff --cached
git commit -m "test: valida persistencia JPA e mensagens Kafka com Testcontainers"
git push
```
