# DEFINE: Free Edition Bronze — modo Volume além de Kafka streaming

> Adicionar um modo de ingestão Bronze baseado em Unity Catalog Volume, para que o pipeline rode no Databricks Free Edition, cujo compute serverless não alcança o Kafka local

## Metadata

| Attribute | Value |
|-----------|-------|
| **Feature** | FREE_EDITION_BRONZE |
| **Date** | 2026-06-17 |
| **Author** | define-agent |
| **Status** | ✅ Shipped (2026-06-17) |
| **Clarity Score** | 15/15 |
| **Predecessor** | BRAINSTORM_FREE_EDITION_BRONZE.md |

---

## Problem Statement

O `pipeline_bronze.ipynb` só sabe ler de Kafka via Structured Streaming, e o compute
serverless do Databricks Free Edition restringe outbound network a uma allowlist fixa e
não-customizável de domínios — não há forma de abrir `kafka:9092` (Docker local) a partir
dele. Sem um modo de ingestão alternativo, o pipeline inteiro é inexecutável em Free Edition,
mesmo que Unity Catalog, CLI e DABs funcionem normalmente lá.

---

## Target Users

| User | Role | Pain Point |
|------|------|------------|
| Usuário rodando no Databricks Free Edition | Quer validar Silver/Gold/Liquid Clustering sem custo de workspace pago | Bronze nunca inicia — `spark.readStream.format("kafka")` não consegue conectar ao broker local |
| Usuário rodando localmente com docker-compose (modo v1.0.1, já validado) | Quer continuar usando o pipeline real de streaming CDC | Não pode perder a capacidade de streaming real ao adicionar suporte a Free Edition |

---

## Goals

| Priority | Goal |
|----------|------|
| **MUST** | Bronze aceita um novo modo de ingestão a partir de um Unity Catalog Volume, sem reescrever a lógica de contrato/DDL/MERGE |
| **MUST** | O modo Kafka streaming existente (v1.0.1) permanece 100% funcional, sem regressão |
| **MUST** | Os 20 domínios podem ser exportados do Kafka (pós-SMT) para o Volume, preservando `__op`/`__source_ts_ms` |
| **SHOULD** | `scripts/preflight_unity_catalog.sh` provisiona o novo schema/Volume de forma idempotente, junto com o resto |
| **SHOULD** | `databricks.yml` permite escolher o modo de ingestão por variável, sem editar os 20 tasks manualmente |
| **COULD** | Documentar no CLAUDE.md a existência dos dois modos, para quem ler o projeto pela primeira vez |

---

## Success Criteria

- [ ] `scripts/export_kafka_to_volume.py` exporta os 20 tópicos `pg.public.*` para Parquet local, com 100% dos campos que o Bronze espera (payload decodificado + `__op` + `__source_ts_ms`)
- [ ] Novo schema `landing` e Volume `kafka_export` criados por `scripts/preflight_unity_catalog.sh --target {dev|prod}`, idempotente (re-execução não falha nem duplica)
- [ ] `pipeline_bronze.ipynb` com `source_mode=volume` produz o mesmo schema e a mesma contagem de linhas em Bronze que `source_mode=kafka` produziria para o mesmo conjunto de dados
- [ ] Rodar a task Bronze duas vezes em `source_mode=volume` resulta em zero registros duplicados (idempotência via `MERGE INTO ... WHEN NOT MATCHED`, sem depender de checkpoint)
- [ ] `source_mode=kafka` (default) continua passando os mesmos testes que já passava em v1.0.1 — sem regressão
- [ ] `databricks.yml` expõe uma variável `bronze_source_mode` (default `kafka`), repassada via `base_parameters` para as 20 tasks Bronze

---

## Acceptance Tests

| ID | Scenario | Given | When | Then |
|----|----------|-------|------|------|
| AT-001 | Export completo dos 20 domínios | Kafka local rodando com os 20 tópicos `pg.public.*` populados (estado pós-v1.0.1) | `python3 scripts/export_kafka_to_volume.py` é executado | 20 arquivos/diretórios Parquet são gerados, um por domínio, com `__op` e `__source_ts_ms` presentes em cada linha |
| AT-002 | Fidelidade ao schema Avro pós-SMT | Um tópico Kafka com mensagens Avro decodificadas via Schema Registry | O export roda para esse tópico | As colunas do Parquet resultante são idênticas às colunas que `from_avro` produziria no notebook (mesmos nomes, mesmos tipos) |
| AT-003 | Provisionamento do schema/Volume novo | `scripts/preflight_unity_catalog.sh --target dev` nunca rodou com a versão atualizada | O script é executado | Catalog, 4 schemas de dados, `checkpoints` (+2 Volumes), e o novo `landing` (+Volume `kafka_export`) existem ao final |
| AT-004 | Idempotência do preflight script | `landing.kafka_export` já existe (execução anterior) | O script é executado de novo | Sai com mensagem "already exists" para `landing`/`kafka_export`, sem erro, exit 0 |
| AT-005 | Bronze em modo `volume` — primeira execução | Parquet de `payment_events` presente em `/Volumes/<catalog>/landing/kafka_export/payment_events/` | Task Bronze roda com `source_mode=volume` | Tabela `bronze.payment_events` populada com a mesma contagem de linhas do Parquet de origem |
| AT-006 | Bronze em modo `volume` — idempotência | AT-005 já executado uma vez | A mesma task Bronze roda de novo, sem nenhum dado novo no Volume | Contagem de linhas em `bronze.payment_events` não muda — zero duplicatas |
| AT-007 | Bronze em modo `kafka` sem regressão | Ambiente local com Kafka rodando (v1.0.1) | Task Bronze roda com `source_mode=kafka` (ou omitido, usando o default) | Comportamento idêntico ao validado em v1.0.1 — streaming, checkpoint, MERGE funcionando |
| AT-008 | `databricks.yml` repassa o modo corretamente | Variável `bronze_source_mode=volume` definida no target | `databricks bundle validate` roda | Todas as 20 tasks Bronze recebem `source_mode=volume` em `base_parameters` |
| AT-009 | Contrato/DDL inalterados entre os dois modos | Mesmo `contract_path` usado nos dois modos | Bronze roda em `kafka` e depois em `volume` (domínios diferentes, dados equivalentes) | O DDL gerado por `to_create_table_ddl()` é idêntico nos dois casos — nenhuma branch de contrato por modo |
| AT-010 | Domínio sem dados no Volume | Export não gerou Parquet para um domínio (ex.: tabela vazia no Postgres) | Task Bronze roda em `source_mode=volume` para esse domínio | Task não falha — trata a ausência de dados como "nada a inserir", análogo ao `batch_df.isEmpty()` já existente no modo Kafka |

---

## Out of Scope

- Export incremental / sincronização contínua — é um snapshot único do microcosmo (129k registros), não um sync recorrente
- Suporte a formatos além de Parquet (ex.: JSON) no export
- Automação do upload do Parquet para o Volume via CI/CD — `databricks fs cp` manual é aceitável
- Validação ao vivo contra um workspace Databricks real durante o `/build` — mesma restrição aplicada em v1.0.1 (só `bash -n`/sintaxe; testes reais ficam para o usuário rodar manualmente, incluindo a verificação final no Free Edition)
- Mudar a lógica de MERGE/idempotência do Bronze — ela já é compatível com ambos os modos, sem alteração necessária
- Migrar Silver/Gold para também terem um `source_mode` — esta feature cobre só Bronze; Silver/Gold já leem de Bronze (Delta), que é igual nos dois modos

---

## Constraints

| Type | Constraint | Impact |
|------|------------|--------|
| Technical | Free Edition: outbound network restrito a allowlist fixa, não customizável | Bronze não pode usar Kafka streaming no Free Edition — motivo desta feature |
| Technical | Free Edition: máximo de 5 job tasks concorrentes por conta | As 37 tasks do DABs rodam em filas de 5 — mais lento, não bloqueante; não afeta a lógica desta feature, só o tempo total de execução |
| Technical | `confluent-kafka[avro,schemaregistry]` já é dependência declarada (`pyproject.toml`) | Sem novas dependências externas para o script de export |
| Technical | Bronze deve permanecer imutável (`WHEN NOT MATCHED THEN INSERT` apenas) em ambos os modos | A lógica de MERGE não pode ser alterada — só a fonte de leitura muda |
| Process | Sem chamadas ao vivo contra Databricks real durante `/build` (mesma regra de v1.0.1) | Validação E2E completa no Free Edition fica para o usuário confirmar manualmente após o build |

---

## Technical Context

| Aspect | Value | Notes |
|--------|-------|-------|
| **Deployment Location** | `scripts/export_kafka_to_volume.py` (novo), `notebooks/pipeline_bronze.ipynb` (modificado), `databricks.yml` (modificado), `scripts/preflight_unity_catalog.sh` (modificado) | Mudança cross-cutting: script novo + notebook + IaC + provisioning |
| **KB Domains** | ADR-02 (SMT/Bronze flat records), Data Contracts (`contracts/*.yml`), ADR-04 (Liquid Clustering) | Nenhum desses padrões muda — só a fonte de leitura do Bronze |
| **IaC Impact** | Novo recurso Unity Catalog: schema `landing` + Volume `kafka_export`, em `scripts/preflight_unity_catalog.sh` | Mesma convenção idempotente (`ensure_schema`/`ensure_volume`) já usada para `checkpoints` |

---

## Data Contract

### Source Inventory

| Source | Type | Volume | Freshness | Owner |
|--------|------|--------|-----------|-------|
| Tópicos Kafka `pg.public.*` (20) | Kafka, Avro pós-SMT | 129,353 registros total | Snapshot único (não é sync contínuo) | export_kafka_to_volume.py |

### Schema Contract

| Column | Type | Constraints | PII? |
|--------|------|-------------|------|
| `<merge_key>` (varia por domínio) | conforme `contracts/*.yml` | NOT NULL (rejeitado em Bronze se nulo) | Depende do domínio (ex.: CPF é PII) |
| `__op` | STRING | Vem do SMT `ExtractNewRecordState` | Não |
| `__source_ts_ms` | LONG | Vem do SMT `ExtractNewRecordState` | Não |

> Schema completo por domínio já existe em `contracts/*.yml` — esta feature não altera nenhum
> contrato, só a fonte física de onde os dados chegam até o Bronze.

### Freshness SLAs

| Layer | Target | Measurement |
|-------|--------|--------------|
| Bronze (modo `volume`) | Snapshot único, sem SLA de frescor — é uma demonstração, não um pipeline contínuo | N/A |
| Bronze (modo `kafka`, inalterado) | Mesma SLA já validada em v1.0.1 | Idêntica |

### Completeness Metrics

- 100% dos 20 domínios exportados devem aparecer no Volume `kafka_export`
- Zero registros com `merge_key` nulo chegando ao Bronze (mesma regra de qualidade já existente, válida nos dois modos)

---

## Assumptions

| ID | Assumption | If Wrong, Impact | Validated? |
|----|------------|---------------------|------------|
| A-001 | `confluent-kafka` (já usado/declarado) consegue consumir os 20 tópicos locais do início ao fim sem timeout | Precisaria de paginação/retry adicional no script de export | [ ] |
| A-002 | Databricks Free Edition permite criar um 5º schema (`landing`) sob o mesmo catalog/metastore, sem limite de contagem | Precisaria reaproveitar um schema existente (Approach B do brainstorm, rejeitada) | [ ] |
| A-003 | Upload de Parquet para um Unity Catalog Volume via `databricks fs cp`/CLI funciona normalmente no Free Edition (Volumes não são afetados pela restrição de rede, que é só para o compute) | Precisaria de um mecanismo de upload alternativo (ex.: UI do workspace) | [ ] |
| A-004 | O limite de 5 job tasks concorrentes não causa falha, só serializa a execução das 37 tasks do DABs | Precisaria reestruturar o DABs em jobs menores/sequenciais explícitos | [ ] |

**Note:** Validar A-002, A-003 e A-004 exige acesso real ao workspace Free Edition — fora do
escopo do `/build` (ver Constraints/Out of Scope). O usuário deve confirmar manualmente após
o deploy.

---

## Clarity Score Breakdown

| Element | Score (0-3) | Notes |
|---------|-------------|-------|
| Problem | 3 | Causa raiz confirmada via documentação oficial + comunidade; impacto específico (Bronze não inicia) |
| Users | 3 | Dois personas com pain points claramente distintos e não-conflitantes |
| Goals | 3 | MUST/SHOULD/COULD bem priorizados, todos derivados das decisões do brainstorm |
| Success | 3 | Critérios mensuráveis (contagem de linhas, idempotência, paridade de schema) |
| Scope | 3 | Out of scope explícito e justificado em 6 itens |
| **Total** | **15/15** | |

**Minimum to proceed: 12/15** — atendido

---

## Open Questions

None — ready for Design.

---

## Revision History

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | 2026-06-17 | define-agent | Initial version, extraído de BRAINSTORM_FREE_EDITION_BRONZE.md |
| 1.1 | 2026-06-17 | ship-agent | Shipped and archived |

---

## Next Step

**Ready for:** `/design .claude/sdd/features/DEFINE_FREE_EDITION_BRONZE.md`
