# KB: Kafka CDC — Fundamentals
# Knowledge base for ai-kafka-microbatch agents

## What is CDC via WAL

Change Data Capture (CDC) via Write-Ahead Log captures database changes by
reading the internal transaction log (WAL), without running additional queries
on the database.

PostgreSQL writes every change to the WAL before applying it to data.
With `wal_level=logical`, the WAL includes enough information to reconstruct
the before and after state of each row.

## How Debezium reads the WAL

1. Creates a replication slot (`debezium_slot`) in PostgreSQL
2. The slot retains the WAL until Debezium confirms the read
3. Debezium uses the `pgoutput` plugin (native to PostgreSQL 10+) to
   decode the WAL into structured events
4. Each event contains: table, operation (c/u/d/r), before, after, metadata

## CDC operations

| op | Meaning | before | after |
|----|---------|--------|-------|
| c  | CREATE (INSERT) | null | new record |
| u  | UPDATE | previous state | state after |
| d  | DELETE | previous state | null |
| r  | READ (initial snapshot) | null | current record |

## ExtractNewRecordState (SMT)

Transforms the complex Debezium payload into a flat payload:

**Without the transform:**
```json
{
  "schema": { ... },
  "payload": {
    "before": { "id": 1, "nome": "Ana" },
    "after":  { "id": 1, "nome": "Ana Lima" },
    "source": { "ts_ms": 1715695200000, ... },
    "op": "u"
  }
}
```

**With the transform:**
```json
{
  "id": 1,
  "nome": "Ana Lima",
  "__op": "u",
  "__source_ts_ms": 1715695200000
}
```

`__op` preserves the operation type. `__source_ts_ms` é o timestamp do evento
no PostgreSQL — usado nos Silver models para ordenar eventos e filtrar a janela
incremental (`source_ts_ms DESC` no ROW_NUMBER, `source_ts_ms > MAX(source_ts_ms)` no filtro).

### delete.handling.mode=rewrite + drop.tombstones=false

DELETEs não são descartados. O SMT os reescreve como eventos normais com `__op=d`
no payload — o registro deleted chega no Kafka com seus valores e `__op=d`.
Tombstones (mensagens com value=null) são mantidos (`drop.tombstones=false`).
Os Silver models filtram `op != 'd'` para excluir DELETEs do estado atual.

## Publication e replication slot

```sql
-- Publication cobre todos os 20 domínios (criada por scripts/init.sql)
CREATE PUBLICATION dbz_publication FOR TABLE
    payment_events, orders, payments, order_items, gps_events,
    order_status, routes, receipts, driver_shifts, search_events,
    recommendations, support_tickets, users_mongo, users_mssql,
    restaurants, drivers, products, menu_sections, ratings, inventory;

-- O replication slot é criado automaticamente pelo Debezium no startup
-- slot.name=debezium_slot
```

## Initial snapshot

On first run, Debezium performs a full snapshot of the tables before
starting WAL streaming. All existing records are emitted with `op=r`.
The snapshot ensures the landing starts with the complete database state.

## Tópicos Kafka gerados

Formato: `{topic.prefix}.{postgres_schema}.{table}` — com `topic.prefix=pg`:

```
pg.public.payment_events    pg.public.orders         pg.public.payments
pg.public.order_items       pg.public.gps_events     pg.public.order_status
pg.public.routes            pg.public.receipts       pg.public.driver_shifts
pg.public.search_events     pg.public.recommendations pg.public.support_tickets
pg.public.users_mongo       pg.public.users_mssql    pg.public.restaurants
pg.public.drivers           pg.public.products       pg.public.menu_sections
pg.public.ratings           pg.public.inventory
```

20 tópicos no total. `order_items` vai para o conector `sinkitems` (buffer maior).
Os outros 19 vão para o conector `sink`.

## Configurações críticas do conector Debezium

Configurações que afetam diretamente o tipo e formato dos dados no Kafka:

| Configuração | Valor | Efeito |
|---|---|---|
| `decimal.handling.mode` | `double` | NUMERIC/DECIMAL → float64 no Avro (não BYTES) |
| `time.precision.mode` | `connect` | Timestamps → milissegundos (não microssegundos) |
| `interval.handling.mode` | `string` | INTERVAL PostgreSQL → string legível |
| `key.converter` | `JsonConverter` | Chaves dos eventos em JSON sem schema (não Avro) |
| `value.converter` | `AvroConverter` | Payload em Avro via Schema Registry |
| `value.converter.auto.register.schemas` | `true` | Registra schema automaticamente no Registry |

`decimal.handling.mode=double` combinado com timestamps que chegam como int ou float
(17% int, 83% float em notação científica) é o motivo do ADR-13:
cast `::FLOAT` antes de `::BIGINT` nos Bronze models.

## Adicionando uma nova tabela

```sql
-- 1. Criar a tabela no PostgreSQL
CREATE TABLE nova_tabela (...);

-- 2. Adicionar à publication
ALTER PUBLICATION dbz_publication ADD TABLE nova_tabela;

-- 3. Registrar nos conectores via REST (envsubst resolve ${VAR} do .env)
envsubst < connectors/debezium.json | \
  curl -X PUT http://localhost:8083/connectors/debezium-postgres-cdc/config \
  -H "Content-Type: application/json" -d @-

-- 4. Executar sync_metadata.py para registrar em CONFIG.TABLE_METADATA
python scripts/sync_metadata.py
```

## Replication slot — production warning

The slot retains the WAL while Debezium is stopped. In production:
- Monitor: `SELECT * FROM pg_replication_slots;`
- Configure: `max_slot_wal_keep_size = '1GB'` to avoid disk full
- Alert if `pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)` > threshold
