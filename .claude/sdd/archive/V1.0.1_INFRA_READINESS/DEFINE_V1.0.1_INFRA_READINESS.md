# DEFINE: v1.0.1 Infra Readiness — gaps reais antes do primeiro `make up`

> Completar o PostgreSQL CDC source, registrar o Debezium connector e provisionar o Unity Catalog — e corrigir a ADR-02 para refletir o que o código já faz

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | V1.0.1_INFRA_READINESS |
| **Date** | 2026-06-17 |
| **Author** | define-agent |
| **Status** | ✅ Shipped (2026-06-17) |
| **Clarity Score** | 15/15 |
| **Predecessor** | BRAINSTORM_V1.0.1_INFRA_READINESS.md |

---

## Problem Statement

O pipeline não roda ponta a ponta hoje: `sql/init.sql` não cria nenhuma das 20 tabelas PostgreSQL (só extensão + replication slot + publication), e não existe `connectors/debezium.json` nem `scripts/register_connectors.sh` — então `make up` sobe os containers mas `make produce-initial` falha (tabelas inexistentes) e o Kafka nunca recebe CDC (connector nunca registrado). Em paralelo, nada no repositório cria o Catalog, os 4 Schemas ou os Volumes do Unity Catalog, então o primeiro `databricks bundle deploy` falharia na primeira `CREATE TABLE`. Por fim, a ADR-02 documentada (`CLAUDE.md`, `03_design.md`, `02_define.md`) diz "Bronze sem SMT, envelope raw" mas o código real (`pipeline_bronze.ipynb`, `pipeline_silver.ipynb`, contratos) já implementa "Bronze com SMT, dados flat" — uma contradição que vai confundir qualquer adaptação futura do `debezium.json`.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Engenheiro rodando o stack localmente | Executa `make up` + `make produce-initial` | Containers sobem mas o load de dados falha por tabelas inexistentes; CDC nunca chega ao Kafka |
| Quem faz o primeiro `databricks bundle deploy` | Deploy inicial em dev/prod | Primeira `CREATE TABLE` no notebook Bronze falha por catalog/schema/volume inexistentes |
| Próximo agente/revisor lendo a ADR-02 | Consome `CLAUDE.md`/`03_design.md` como fonte de verdade | Doc diz "sem SMT" — risco de tentar adicionar lógica de unwrap no Silver que já não é necessária, ou de gerar um `debezium.json` inconsistente com o `pipeline_bronze.ipynb` real |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | `sql/init.sql` cria as 20 tabelas com schema validado contra os 19 contratos YAML (tipos, PKs, casos especiais) |
| **MUST** | `connectors/debezium.json` registra 1 connector PostgreSQL→Kafka mantendo a SMT `ExtractNewRecordState` |
| **MUST** | `scripts/register_connectors.sh` registra só `debezium-postgres-cdc` (sem Sink Connectors), seta BACKWARD compatibility inline, idempotente (HTTP 409 tratado como sucesso) |
| **MUST** | `scripts/preflight_unity_catalog.sh` cria Catalog + 4 Schemas (bronze/silver/gold/quarantine) + Volume de checkpoint, para `dev` e `prod`, idempotente |
| **MUST** | ADR-02 corrigida em `CLAUDE.md`, `03_design.md` e `02_define.md` para refletir "usa SMT" |
| **MUST** | TD-01/TD-06 (JMX, já resolvido no código) e TD-04 (Volumes) fechados em `06_retrospective.md` apontando para os artefatos certos |
| **SHOULD** | `file_manifest` de `03_design.md` corrigido: `sql/init.sql` (não `scripts/init.sql`) e `scripts/set_compatibility.sh` removido da lista (dobrado em `register_connectors.sh`) |
| **COULD** | Makefile ganha um target `register-connectors` chamando o novo script (conveniência, não bloqueante) |

**Priority Guide:**
- **MUST** = sem isso o primeiro `make up` → `bundle deploy` continua quebrado
- **SHOULD** = limpeza de documentação relacionada, não bloqueia a execução
- **COULD** = conveniência de DX, corta primeiro se o tempo apertar

---

## Success Criteria

- [ ] `docker compose up -d --wait` seguido de uma query em `information_schema.tables` retorna as 20 tabelas de domínio em `public`
- [ ] `make produce-initial` roda sem erro "relation does not exist" e popula as 20 tabelas
- [ ] `curl http://localhost:8083/connectors` retorna exatamente `["debezium-postgres-cdc"]`, e `GET /connectors/debezium-postgres-cdc/status` reporta `"state": "RUNNING"`
- [ ] Reexecutar `scripts/register_connectors.sh` uma segunda vez não falha (HTTP 409 tratado como esperado, não como erro)
- [ ] `scripts/preflight_unity_catalog.sh --target dev` e `--target prod` saem com exit code 0 e são idempotentes (segunda execução não falha nem duplica recursos)
- [ ] `CLAUDE.md`, `03_design.md` e `02_define.md` não contêm mais a frase "SMT ... NÃO é usada" / "no SMT" para a Bronze — o texto passa a descrever a SMT como decisão ativa
- [ ] `06_retrospective.md`: TD-06 (JMX, rótulo do usuário TD-01) e TD-04 (Volumes, rótulo do usuário TD-07) aparecem marcados como resolvidos, cada um citando o artefato que os resolve

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Schema PostgreSQL completo | `sql/init.sql` atualizado, containers parados | `make up` | As 20 tabelas existem em `public`, com tipos batendo com `contracts/*.yml` |
| AT-002 | Load inicial funciona | AT-001 satisfeito | `make produce-initial` | 0 erros de "relation does not exist"; contagem de linhas por tabela > 0 |
| AT-003 | Connector único registrado | Stack up, `register_connectors.sh` executado | `curl localhost:8083/connectors` | Retorna lista com exatamente 1 item: `debezium-postgres-cdc` |
| AT-004 | SMT ativa no connector | AT-003 satisfeito | `curl localhost:8083/connectors/debezium-postgres-cdc/config` | Resposta contém `"transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState"` |
| AT-005 | Registro idempotente | AT-003 já executado uma vez | Rodar `register_connectors.sh` de novo | Script termina com sucesso (HTTP 409 tratado como "já existe", não como falha) |
| AT-006 | Schema Registry BACKWARD | Stack up, script executado | `curl localhost:8081/config` | `{"compatibilityLevel":"BACKWARD"}` |
| AT-007 | Pre-flight Unity Catalog — dev | Workspace Databricks acessível via CLI autenticado | `scripts/preflight_unity_catalog.sh --target dev` | Catalog `ubereats_dev`, 4 schemas e o volume `/Volumes/ubereats_dev/checkpoints` existem |
| AT-008 | Pre-flight idempotente | AT-007 já executado | Rodar de novo com `--target dev` | Exit code 0, nenhum erro "already exists" não tratado |
| AT-009 | ADR-02 consistente | Nenhuma | `grep -i "ExtractNewRecordState" CLAUDE.md 03_design.md 02_define.md` | Texto descreve a SMT como usada, sem contradizer `pipeline_bronze.ipynb` |
| AT-010 | TDs fechados | Nenhuma | Ler `06_retrospective.md` | TD-06 e TD-04 marcados resolvidos, cada um citando o arquivo/script que o resolve |

---

## Out of Scope

- Validação end-to-end do JMX/Prometheus (confirmar que as métricas realmente chegam no Grafana) — é teste de infraestrutura, não gap de implementação
- `scripts/set_compatibility.sh` como arquivo dedicado — a única ação (PUT BACKWARD) fica inline em `register_connectors.sh`
- Qualquer Sink Connector (Snowflake ou outro) — Databricks lê Kafka direto via Structured Streaming (ADR-01)
- Migrar `debezium_publication FOR ALL TABLES` para lista explícita de tabelas (`dbz_publication FOR TABLE ...` como na referência)
- Declarar Catalog/Schemas/Volumes como `resources` do Databricks Asset Bundle (`databricks.yml`) — fica como script imperativo de CLI nesta iteração
- Resolver TD-02 (`pipeline_users.ipynb` não usa `pipeline_silver.ipynb`) e TD-03 (`order_items` `max_offsets` tuning) — ficam para outra iteração

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | `connectors/debezium.json` deve manter `transforms.unwrap.type: ExtractNewRecordState` | `pipeline_bronze.ipynb` já espera schema pós-SMT (`business fields + __op + __source_ts_ms`) — removê-la quebraria o parsing Avro existente |
| Technical | `register_connectors.sh` registra só 1 connector (`debezium-postgres-cdc`) | ADR-01: sem Sink Connector, Databricks consome o tópico Kafka direto |
| Technical | Publication continua `debezium_publication FOR ALL TABLES` | Já em uso no `sql/init.sql` parcial; evita editar a lista a cada novo domínio |
| Technical | `sql/init.sql` é o path real montado em `docker-compose.yml` (`./sql/init.sql:/docker-entrypoint-initdb.d/init.sql:ro`) | Não mover para `scripts/init.sql` (path do `file_manifest`, que está errado) — só corrigir o manifest |
| Technical | Pre-flight do Unity Catalog é script bash + Databricks CLI, não notebook nem `resources` do bundle | `databricks.yml` hoje só declara `resources.jobs`; CLI é mais simples de tornar idempotente nesta iteração |
| Technical | Tipos do `sql/init.sql` devem bater com os 19 contratos YAML (`UUID`→`string`, `INTEGER`→`integer`, `TIMESTAMPTZ`→`timestamp`, `JSONB`→`string`) | Evita drift entre o schema Postgres e o que `contracts/spark_schema.py` gera para Bronze |
| Documentation | ADR-02 corrigida em 3 arquivos (`CLAUDE.md`, `03_design.md`, `02_define.md`) | Os 3 lugares hoje repetem o texto "sem SMT" — corrigir só um deixaria os outros dois inconsistentes |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `sql/init.sql` (expandir existente); `connectors/debezium.json`, `scripts/register_connectors.sh`, `scripts/preflight_unity_catalog.sh` (novos diretórios) | `connectors/` e `scripts/` ainda não existem no projeto — precisam ser criados |
| **KB Domains** | `data-engineering` (CDC/Debezium), `spark` (contracts ↔ DDL), `streaming-engineer` (SMT, conector) | Padrões de schema PostgreSQL↔contrato e de configuração Debezium |
| **IaC Impact** | Nenhuma mudança em `docker-compose.yml` (mount de `sql/init.sql` já existe); novo script `preflight_unity_catalog.sh` cria recursos no workspace Databricks (Catalog/Schema/Volume) via CLI, fora do Terraform/IaC formal do projeto (não há um) |

**Why This Matters:**

- **Location** → `connectors/` e `scripts/` são diretórios novos; o Design precisa confirmar que nada mais espera esses paths em outro lugar (`file_manifest.infrastructure` em `03_design.md` já antecipa ambos, exceto o `init.sql` que está com path errado)
- **KB Domains** → padrões de CDC/Debezium e de geração de DDL a partir de contratos já existem no projeto e devem ser seguidos, não reinventados
- **IaC Impact** → o pre-flight do Unity Catalog é a única peça que toca infraestrutura fora do Docker Compose local — Design deve detalhar autenticação (`DATABRICKS_HOST`/`DATABRICKS_TOKEN` do `.env`) e tratamento de erro quando o CLI não está autenticado

---

## Data Contract

### Source Inventory
| Source | Type | Volume | Freshness | Owner |
|--------|------|--------|-----------|-------|
| PostgreSQL (20 tabelas, WAL logical) | CDC source | 129.353 registros / 100 arquivos JSON | Near-real-time via Debezium após `load_to_postgres.py` | Este projeto |
| Kafka (`pg.public.*`, 20 tópicos) | CDC sink intermediário | Mesmo volume, em Avro | Streaming contínuo após registro do connector | Confluent Schema Registry + Kafka Connect |

### Schema Contract
| Column | Type | Constraints | PII? |
|--------|------|-------------|------|
| `*.{merge_key}` (ex: `event_id`, `order_id`, `stock_id`) | conforme `contracts/*.yml` | `NOT NULL`, chave primária na tabela Postgres | Não (UUIDs/IDs sintéticos) |
| `orders.user_key` | `VARCHAR(20)` (CPF) | Nullable, formato `000.000.000-00` | Sim — CPF é PII brasileiro |
| `orders.restaurant_key` | `VARCHAR(20)` (CNPJ) | Nullable, formato `00.000.000/0000-00` | Não (identificador de empresa) |
| `__op`, `__source_ts_ms` | adicionados pela SMT, não existem no Postgres | Gerados pelo Debezium, não pelo `init.sql` | Não |

> Fonte de verdade definitiva é `contracts/*.yml` — o `sql/init.sql` deve ser gerado/validado a partir dela, não o contrário.

### Freshness SLAs
| Layer | Target | Measurement |
|-------|--------|-------------|
| PostgreSQL → Kafka | Segundos após commit no WAL (Debezium `snapshot.mode: initial` + streaming contínuo) | `pg_replication_slots` lag |
| Kafka → Bronze | Próximo `trigger(availableNow=True)` do `pipeline_bronze.ipynb` | DABs job run timestamp |

### Completeness Metrics
- 100% das 20 tabelas presentes em `information_schema.tables` após `make up`
- 0 erros de schema mismatch entre `sql/init.sql` e `contracts/*.yml` (tipo, nullability dos campos de negócio)
- Exatamente 1 connector ativo em `RUNNING`, capturando os 20 tópicos `pg.public.*`

### Lineage Requirements
- Nenhuma mudança de lineage nesta feature — `sql/init.sql`/`debezium.json` só viabilizam o lineage já desenhado (`contracts/*.yml` → Bronze → Silver → Gold)

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|------------------|------------|
| A-001 | Os tipos do `sql/init.sql` adaptado (`UUID`, `VARCHAR`, `TIMESTAMPTZ`, `JSONB`, `NUMERIC`) são compatíveis com os tipos abstratos dos 19 contratos (`string`/`integer`/`long`/`timestamp`) | `to_create_table_ddl()` geraria DDL incompatível com os dados reais do Postgres | [x] Verificado campo a campo no brainstorm para `order_status`, `search_events`, `receipts`, `inventory` |
| A-002 | O Databricks CLI está instalado e autenticado (`DATABRICKS_HOST`/`DATABRICKS_TOKEN` válidos) no ambiente onde `preflight_unity_catalog.sh` roda | Script falharia em qualquer chamada `databricks catalogs/schemas/volumes create` | [ ] Depende de configuração local do usuário — não validável neste momento |
| A-003 | A imagem `Dockerfile.connect` (`cp-kafka-connect:7.7.1` + `debezium-connector-postgresql:2.7.1.Final`) já suporta `ExtractNewRecordState` e Avro converter sem plugins adicionais | `register_connectors.sh` falharia ao registrar com `transforms.unwrap` | [x] Confirmado — `ExtractNewRecordState` é parte do connector Debezium core; Avro converter vem no `cp-kafka-connect` base |
| A-004 | Nenhum outro notebook/script além de `pipeline_bronze.ipynb`/`pipeline_silver.ipynb` depende do texto atual da ADR-02 ("sem SMT") | Corrigir a ADR-02 quebraria alguma lógica que assume envelope raw | [x] Confirmado — `pipeline_silver.ipynb` não tem nenhuma lógica de unwrap (`before`/`after`/`from_avro`) |

**Note:** A-002 (autenticação Databricks CLI) é a única assumption não validável agora — deve ser tratada no Design com uma checagem explícita (`databricks auth describe` ou similar) e mensagem de erro clara no script, não com retry silencioso.

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | 3 causas raiz nomeadas (schema Postgres ausente, connector nunca registrado, Unity Catalog não provisionado) + a contradição de ADR-02, todas confirmadas lendo o código real, não só a retrospectiva |
| Users | 3 | 3 personas com dor específica e rastreável a um comando ou arquivo concreto |
| Goals | 3 | MUST/SHOULD/COULD com critérios técnicos explícitos (SMT mantida, 1 connector, idempotência) |
| Success | 3 | 7 critérios mensuráveis, todos verificáveis por comando (`curl`, `grep`, `make`, query SQL) |
| Scope | 3 | Out of scope explícito com 6 itens, incluindo por que cada um foi descartado (YAGNI do brainstorm) |
| **Total** | **15/15** | |

**Minimum to proceed: 12/15** ✅

---

## Open Questions

Nenhuma — pronto para Design. A única assumption não validada (A-002, autenticação do Databricks CLI) não bloqueia o Design; deve ser endereçada como tratamento de erro explícito no script de pre-flight.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-17 | define-agent | Initial version, extraído de BRAINSTORM_V1.0.1_INFRA_READINESS.md |
| 1.1 | 2026-06-17 | ship-agent | Shipped and archived |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_V1.0.1_INFRA_READINESS.md`
