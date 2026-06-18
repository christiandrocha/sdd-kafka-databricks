# BRAINSTORM: Free Edition Bronze — modo Volume além de Kafka streaming

> Exploratory session to clarify intent and approach before requirements capture

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | FREE_EDITION_BRONZE |
| **Date** | 2026-06-17 |
| **Author** | brainstorm-agent |
| **Status** | ✅ Shipped (2026-06-17) |

---

## Initial Idea

**Raw Input:** "Preciso executar o projeto no Databricks Free Edition."

**Context Gathered:**
- Pesquisa confirmou (docs.databricks.com/aws/en/getting-started/free-edition-limitations +
  thread da comunidade): o compute serverless do Free Edition restringe outbound network a
  uma allowlist fixa de domínios, **não customizável** (customização só existe no tier
  Enterprise via network policies). Um usuário relatou até `requests.get()` para domínios
  genéricos sendo bloqueado.
- Isso bloqueia `notebooks/pipeline_bronze.ipynb` no formato atual: a célula de leitura faz
  `spark.readStream.format("kafka").option("kafka.bootstrap.servers", kafka_bootstrap)...`
  apontando para o Kafka local (Docker, `localhost:9092`/`kafka:9092`) — inalcançável a
  partir do compute do Free Edition.
- O que funciona normalmente no Free Edition: Unity Catalog (1 metastore por conta, múltiplos
  catálogos sem limite documentado), Databricks CLI + PAT, Databricks Asset Bundles
  (`bundle deploy`) — são recursos de workspace, não de conta.
- Limite real (não bloqueante, só relevante para tempo de execução): máximo de 5 job tasks
  concorrentes por conta — os 37 tasks do DABs (20 bronze + 11 silver + 6 gold) vão rodar em
  filas de 5, não em paralelo.
- `confluent-kafka` (extras `avro`, `schemaregistry`) já é dependência declarada em
  `pyproject.toml` — nenhuma dependência nova necessária para o script de export.
- `scripts/preflight_unity_catalog.sh` hoje cria: catalog + 4 schemas de dados
  (bronze/silver/gold/quarantine) + schema `checkpoints` com 2 Volumes (bronze/silver) —
  documentado em CLAUDE.md como **"operational only, no data tables"**. Misturar dados de
  export ali quebraria essa separação já documentada.

**Technical Context Observed (for Define):**

| Aspect | Observation | Implication |
|--------|-------------|--------------|
| Likely Location | `notebooks/pipeline_bronze.ipynb`, novo `scripts/export_kafka_to_volume.py`, `databricks.yml`, `scripts/preflight_unity_catalog.sh` | Mudança cross-cutting: notebook + script novo + IaC + provisioning |
| Relevant KB Domains | Debezium SMT (ADR-02), Data Contracts (`contracts/*.yml`), Liquid Clustering (ADR-04) | A lógica de contrato/DDL/MERGE do Bronze não muda — só a fonte de leitura |
| IaC Patterns | `databricks.yml` (DABs vars + base_parameters), `scripts/preflight_unity_catalog.sh` (bash idempotente) | Seguir os mesmos padrões já estabelecidos em v1.0.1 |

---

## Discovery Questions & Answers

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | O modo Volume deve substituir o Kafka streaming definitivamente, ou conviver como segunda opção? | Conviver lado a lado — widget `source_mode` (`kafka` \| `volume`) | Notebook ganha um branch condicional em vez de uma reescrita; modo Kafka local permanece intacto e validado (v1.0.1) |
| 2 | Os 8 domínios bronze-only também precisam ser exportados, ou só os 11 que alimentam Silver? | Todos os 20 domínios | Paridade completa entre ambiente local e Free Edition; export script e DABs tratam Bronze uniformemente |
| 3 | O export deve ler do Kafka (pós-SMT, com `__op`/`__source_ts_ms`) ou direto do PostgreSQL? | Kafka, pós-SMT | Preserva fidelidade total ao desenho real da CDC (ADR-02) — o Volume fica estruturalmente idêntico ao que o modo Kafka produziria, só como snapshot em vez de streaming |
| 4 | Em modo `volume` (sem checkpoint), repetir a task Bronze duplica registros? | Não — confirmado pelo usuário e validado: o checkpoint nunca foi a garantia de idempotência (só evita reler offsets já consumidos); a garantia real sempre foi `WHEN NOT MATCHED THEN INSERT` do `MERGE INTO`, que vale em ambos os modos | Nenhuma mudança na lógica de MERGE é necessária; a idempotência do Bronze é preservada por design existente |

**Minimum Questions:** 3 — atendido (4 perguntas, incluindo uma de validação técnica)

---

## Sample Data Inventory

| Type | Location | Count | Notes |
|------|----------|-------|-------|
| Input files | `tests/data/` (JSON originais) | 100 arquivos | Não usados diretamente — o export lê dos tópicos Kafka, não dos JSONs originais |
| Related code | `notebooks/pipeline_bronze.ipynb` (célula de parsing Avro) | 1 notebook | Padrão de decodificação Avro (`from_avro`, substring(value,6) para o wire format Confluent) reaproveitado conceitualmente no script de export, em Python |
| Related code | `scripts/register_connectors.sh` (padrão bash idempotente) | 1 script | Convenção de espera/idempotência a seguir no `preflight_unity_catalog.sh` atualizado |
| Dependency | `pyproject.toml` — `confluent-kafka[avro,schemaregistry]` `^2.4` | já declarada | Nenhuma dependência nova necessária |

**How samples will be used:**
- O padrão de decodificação Avro do notebook (wire format Confluent: 1 magic byte + 4 bytes
  schema ID + payload) é replicado no script de export Python via
  `confluent_kafka.schema_registry.avro.AvroDeserializer`.

---

## Approaches Explored

### Approach A: Export Kafka → Parquet → schema `landing` + widget `source_mode` ⭐ Recommended

**Description:** Novo script `scripts/export_kafka_to_volume.py` consome cada um dos 20
tópicos `pg.public.*` do início ao fim (snapshot finito, não stream contínuo) usando
`confluent-kafka` + Schema Registry, grava Parquet local com os mesmos campos que o Bronze já
espera (payload decodificado + `__op` + `__source_ts_ms`). Esses arquivos sobem para um novo
schema `landing` com um Volume `kafka_export` (subpastas por domínio). O notebook
`pipeline_bronze.ipynb` ganha o widget `source_mode` (`kafka` default | `volume`): em modo
`volume`, troca `spark.readStream.format("kafka")` por `spark.read.format("parquet")` no
Volume e chama `merge_to_bronze()` diretamente (uma vez, sem streaming/checkpoint). Toda a
lógica de contrato, DDL e MERGE permanece idêntica. `databricks.yml` ganha a variável
`bronze_source_mode` (default `kafka`), repassada via `base_parameters`.

**Pros:**
- Reaproveita 100% da lógica de contrato/DDL/MERGE já existente e validada
- Modo Kafka local (v1.0.1, já testado end-to-end) permanece intacto, sem regressão
- Mantém a hierarquia `catalog.schema.volume` consistente com bronze/silver/gold/quarantine
- Preserva fidelidade total ao desenho CDC real (campos `__op`/`__source_ts_ms` vindos do SMT)

**Cons:**
- Passo manual: rodar o export localmente + `databricks fs cp` para o Volume antes de cada
  execução Bronze no Free Edition (aceitável — é um snapshot único de demonstração, não um
  pipeline contínuo)
- Reprocessa 100% do Parquet a cada execução (sem skip incremental) — irrelevante para os
  volumes deste microcosmo (máx. 110k linhas)

**Why Recommended:** Menor blast radius (um widget + um branch + um script novo), preserva
o trabalho já validado em v1.0.1, e mantém a separação documentada entre `checkpoints`
(operacional) e dados reais.

---

### Approach B: Reaproveitar o schema/Volume `checkpoints` existente para os dados exportados

**Description:** Mesmo script de export, mas grava direto no Volume `checkpoints.bronze` já
existente, sem criar um schema novo.

**Pros:**
- Um schema/Volume a menos para provisionar

**Cons:**
- Quebra a separação documentada em CLAUDE.md ("checkpoints: operational only, no data
  tables") — exigiria reescrever essa documentação para mentir sobre a própria estrutura
- Conceitualmente confuso: misturar checkpoint de streaming com dados de snapshot batch

**Not recommended** — economia mínima (1 schema) por um custo de clareza arquitetural alto.

---

## Data Engineering Context

### Source Systems

| Source | Type | Volume Estimate | Current Freshness |
|--------|------|------------------|---------------------|
| Tópicos Kafka `pg.public.*` (20) | Kafka (pós-Debezium/SMT, Avro) | 129,353 registros total | Snapshot único — não é um sync contínuo |

### Data Flow Sketch

```text
[PostgreSQL] → [Debezium/Kafka] → [export_kafka_to_volume.py] → [Volume landing.kafka_export]
                                                                          ↓
                                          [pipeline_bronze.ipynb, source_mode=volume]
                                                                          ↓
                                                            [Bronze Delta, mesma MERGE lógica]
```

### Key Data Questions Explored

| # | Question | Answer | Impact |
|---|----------|--------|--------|
| 1 | Export incremental ou snapshot único? | Snapshot único (YAGNI — é demonstração, não sync contínuo) | Sem lógica de "last exported offset" |
| 2 | Formato do arquivo exportado? | Parquet (não JSON) | Schema preservado, leitura nativa no Spark, menor que JSON |
| 3 | Quem consome o Volume? | Só `pipeline_bronze.ipynb` em modo `volume`, no Free Edition | Nenhum outro consumidor a considerar agora |

---

## Selected Approach

| Attribute | Value |
|-----------|-------|
| **Chosen** | Approach A |
| **User Confirmation** | 2026-06-17 |
| **Reasoning** | Menor blast radius, reaproveita lógica existente, preserva modo Kafka local validado em v1.0.1, mantém hierarquia de schemas consistente |

---

## Key Decisions Made

| # | Decision | Rationale | Alternative Rejected |
|---|----------|-----------|------------------------|
| 1 | Widget `source_mode` (`kafka` \| `volume`) em vez de notebook separado | Reaproveita 100% da lógica de contrato/DDL/MERGE; um único arquivo para manter | Dois notebooks (`pipeline_bronze.ipynb` + `pipeline_bronze_volume.ipynb`) — duplicaria lógica |
| 2 | Novo schema `landing` + Volume `kafka_export`, não reaproveitar `checkpoints` | Preserva a separação documentada "checkpoints = operacional, sem dados" | Approach B (reaproveitar `checkpoints`) |
| 3 | Export lê do Kafka pós-SMT, não direto do Postgres | Fidelidade total ao desenho CDC real (`__op`/`__source_ts_ms` vêm do Debezium, não sintetizados) | Export direto do Postgres (mais simples, mas infiel à arquitetura) |
| 4 | Todos os 20 domínios exportados, não só os 11 que alimentam Silver | Paridade completa entre ambiente local e Free Edition | Exportar só os 11 domínios Silver-feeding |
| 5 | Modo `volume` não usa checkpoint — idempotência garantida só pelo `MERGE INTO ... WHEN NOT MATCHED` | Esse já era o mecanismo real de idempotência em ambos os modos; o checkpoint só evita reler offsets Kafka (otimização, não correção) | Sintetizar um checkpoint/marker artificial para modo `volume` |

---

## Features Removed (YAGNI)

| Feature Suggested | Reason Removed | Can Add Later? |
|---------------------|-------------------|-------------------|
| Export incremental (só registros novos desde a última exportação) | É um snapshot único de demonstração, não um sync contínuo | Yes — se o projeto evoluir para uso recorrente |
| Suporte a JSON além de Parquet no export | Parquet já atende; JSON seria redundante | Yes, mas sem necessidade aparente |
| Upload automatizado do export via CI/CD | Passo manual (`databricks fs cp`) é aceitável para um projeto de demonstração | Yes — se o workflow precisar rodar repetidamente |
| Novo job DABs dedicado para "importar + rodar bronze" | As 20 tasks Bronze existentes já cobrem isso, só trocando `source_mode` | No — não há necessidade identificada |

---

## Incremental Validations

| Section | Presented | User Feedback | Adjusted? |
|---------|-----------|----------------|-----------|
| Arquitetura geral (Approach A) | ✅ | Confirmou os 3 pontos: nome do schema/Volume, comportamento de idempotência sem checkpoint, e lembrete sobre o preflight script | Não — só esclarecimentos, sem mudança de direção |
| Comportamento de idempotência em modo `volume` | ✅ | Confirmado: `WHEN NOT MATCHED THEN INSERT` garante idempotência independente do checkpoint | Não |

**Minimum Validations:** 2 — atendido

---

## Suggested Requirements for /define

### Problem Statement (Draft)
O pipeline Bronze atual só lê de Kafka via streaming, o que é inexecutável no Databricks Free
Edition devido à restrição de rede de saída do compute serverless (allowlist fixa, não
customizável) — é preciso um modo alternativo de ingestão que preserve a lógica de
contrato/DDL/MERGE já validada.

### Target Users (Draft)

| User | Pain Point |
|------|------------|
| Usuário rodando o projeto no Databricks Free Edition | Não consegue conectar ao Kafka local — Bronze nunca inicia |
| Usuário rodando localmente com docker-compose (modo já validado em v1.0.1) | Precisa que essa mudança não regrida o que já funciona |

### Success Criteria (Draft)
- [ ] `scripts/export_kafka_to_volume.py` exporta os 20 tópicos `pg.public.*` para Parquet,
  preservando `__op`/`__source_ts_ms`
- [ ] Novo schema `landing` + Volume `kafka_export` criado por
  `scripts/preflight_unity_catalog.sh`
- [ ] `pipeline_bronze.ipynb` aceita `source_mode=volume` e produz o mesmo resultado em
  Bronze que o modo `kafka` (mesma contagem de linhas, mesmo schema, sem duplicatas em
  re-execução)
- [ ] Modo `source_mode=kafka` (default) permanece 100% funcional, sem regressão
- [ ] `databricks.yml` permite escolher `source_mode` por variável/target
- [ ] `databricks bundle validate`/`deploy` funcionam contra um workspace Free Edition real
  (sujeito ao limite de 5 tasks concorrentes — execução mais lenta, não bloqueante)

### Constraints Identified
- Free Edition: outbound network restrito a allowlist fixa, não customizável
- Free Edition: máximo 5 job tasks concorrentes por conta
- `confluent-kafka[avro,schemaregistry]` já é dependência — sem novas dependências externas
- Bronze deve permanecer imutável (`WHEN NOT MATCHED THEN INSERT` apenas) em ambos os modos

### Out of Scope (Confirmed)
- Export incremental / sync contínuo
- Suporte a formatos além de Parquet
- Automação do upload via CI/CD
- Validação ao vivo contra um workspace Databricks real durante o /build (mesma restrição de
  v1.0.1 — só `bash -n`/sintaxe; testes reais ficam para o usuário rodar manualmente)

---

## Session Summary

| Metric | Value |
|--------|-------|
| Questions Asked | 4 |
| Approaches Explored | 2 |
| Features Removed (YAGNI) | 4 |
| Validations Completed | 2 |
| Duration | ~1 sessão |

---

## Next Step

**Ready for:** `/define .claude/sdd/features/BRAINSTORM_FREE_EDITION_BRONZE.md`
