# KB: Observability — Prometheus + Grafana for Kafka + dbt pipelines
# Knowledge base for sdd-kafka-snowflake agents

## Two types of observability

**Infrastructure observability** (this KB):
- Is the pipeline running? Are services healthy?
- Is Kafka keeping up with PostgreSQL event rate?
- Are connectors running or failing?
- Is Dagster executing runs successfully?
- Tool: Prometheus + Grafana

**Data quality observability** (see dbt.md):
- Is the data correct? Fresh? Complete?
- Are Silver tables missing expected rows?
- Did a DELETE propagate correctly to Gold?
- Tool: dbt tests, dbt source freshness, dbt-expectations

Both are necessary. This KB covers infrastructure observability only.

## Architecture

```
Kafka JMX metrics (port 9101)
    └─▶ JMX Exporter (port 5556)
            └─▶ Prometheus (scrapes every 15s)
                    └─▶ Grafana (visualizes + alerts)

Kafka Connect REST API (port 8083)
    └─▶ Prometheus (scrapes /connectors/{name}/status)

Dagster (port 3000)
    └─▶ Prometheus (scrapes /metrics if dagster-prometheus installed)
```

## Critical Kafka metrics for CDC pipelines

### Consumer lag (most important)
```
kafka_consumer_group_lag{topic="pg.public.usuarios", partition="0"}
```
- **lag = 0**: consumer (Snowflake Sink) is keeping up with Debezium
- **lag growing**: Snowflake Sink is falling behind — data is delayed
- **lag alert threshold**: 10,000 messages (configurable)
- **root causes of lag**: Snowflake slow, connector paused, network issue

### Throughput
```
kafka_server_brokertopicmetrics_messagesin_total{topic="pg.public.usuarios"}
```
- Messages produced per second per topic
- Sudden drop → Debezium stopped producing (WAL issue?)
- Sudden spike → bulk operation in PostgreSQL

### Under-replicated partitions
```
kafka_server_replicamanager_underreplicatedpartitions
```
- Should always be 0 in healthy cluster
- > 0 → broker health issue (not relevant for single-broker PoC)

## Kafka Connect metrics

```
# Connector status via REST (3 conectores registrados)
GET http://localhost:8083/connectors/debezium-postgres-cdc/status
GET http://localhost:8083/connectors/sink/status
GET http://localhost:8083/connectors/sinkitems/status

# Prometheus scrape target (custom exporter ou kafka-connect-prometheus-reporter)
kafka_connect_connector_status{connector="debezium-postgres-cdc"} 1.0  # 1=RUNNING
kafka_connect_connector_status{connector="sink"} 1.0
kafka_connect_connector_status{connector="sinkitems"} 1.0
```

## prometheus.yml configuration

```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

rule_files:
  - /etc/prometheus/alert_rules.yml

scrape_configs:
  - job_name: kafka-jmx
    static_configs:
      - targets: ['jmx-exporter:5556']
    relabel_configs:
      - source_labels: [__address__]
        target_label: instance
        replacement: kafka

  - job_name: kafka-connect
    metrics_path: /metrics
    static_configs:
      - targets: ['kafka-connect:8083']

  - job_name: dagster
    metrics_path: /metrics
    static_configs:
      - targets: ['dagster:3000']
```

## alert_rules.yml

```yaml
groups:
  - name: kafka_cdc_alerts
    rules:

      - alert: KafkaConsumerLagHigh
        expr: kafka_consumer_group_lag > 10000
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Consumer lag too high"
          description: "Consumer lag for {{ $labels.topic }} is {{ $value }} messages"

      - alert: KafkaConnectTaskFailed
        expr: kafka_connect_connector_task_status != 1
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Kafka Connect task failed"
          description: "Connector {{ $labels.connector }} task {{ $labels.task }} is not RUNNING"

      - alert: DagsterRunFailed
        expr: dagster_run_status{status="FAILURE"} > 0
        for: 0m
        labels:
          severity: warning
        annotations:
          summary: "Dagster pipeline run failed"
          description: "dbt pipeline run failed — check Dagster UI at http://localhost:3000"
```

## JMX Exporter configuration for Kafka

```yaml
# infra/observability/jmx/kafka-jmx-exporter.yml
lowercaseOutputName: true
lowercaseOutputLabelNames: true

rules:
  # Consumer group lag
  - pattern: 'kafka.consumer<type=consumer-fetch-manager-metrics, client-id=(.+), topic=(.+), partition=(.+)><>records-lag'
    name: kafka_consumer_group_lag
    labels:
      client_id: "$1"
      topic: "$2"
      partition: "$3"

  # Messages in per topic
  - pattern: 'kafka.server<type=BrokerTopicMetrics, name=MessagesInPerSec, topic=(.+)><>Count'
    name: kafka_server_brokertopicmetrics_messagesin_total
    labels:
      topic: "$1"

  # Under-replicated partitions
  - pattern: 'kafka.server<type=ReplicaManager, name=UnderReplicatedPartitions><>Value'
    name: kafka_server_replicamanager_underreplicatedpartitions
```

## Grafana dashboard provisioning

Grafana auto-loads dashboards from JSON files when provisioning is configured:

```yaml
# infra/observability/grafana/provisioning/dashboards/dashboards.yml
apiVersion: 1
providers:
  - name: default
    type: file
    options:
      path: /var/lib/grafana/dashboards
      foldersFromFilesStructure: true
```

```yaml
# infra/observability/grafana/provisioning/datasources/prometheus.yml
apiVersion: 1
datasources:
  - name: Prometheus
    type: prometheus
    url: http://prometheus:9090
    isDefault: true
    editable: false
```

## docker-compose additions for observability

```yaml
  jmx-exporter:
    image: bitnami/jmx-exporter:latest
    container_name: jmx-exporter
    ports:
      - "5556:5556"
    volumes:
      - ./observability/jmx/kafka-jmx-exporter.yml:/opt/jmx-exporter/config.yml
    command: "5556 /opt/jmx-exporter/config.yml"
    environment:
      JMX_HOST: kafka
      JMX_PORT: "9101"
    depends_on:
      kafka:
        condition: service_healthy

  prometheus:
    image: prom/prometheus:v2.49.0
    container_name: prometheus
    ports:
      - "9090:9090"
    volumes:
      - ./observability/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - ./observability/prometheus/alert_rules.yml:/etc/prometheus/alert_rules.yml
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'
      - '--web.enable-lifecycle'
    depends_on:
      - jmx-exporter
      - kafka-connect

  grafana:
    image: grafana/grafana:10.2.0
    container_name: grafana
    ports:
      - "3001:3000"    # 3001 externally to avoid conflict with Dagster on 3000
    environment:
      GF_SECURITY_ADMIN_PASSWORD: admin
      GF_USERS_ALLOW_SIGN_UP: "false"
    volumes:
      - ./observability/grafana/provisioning:/etc/grafana/provisioning
      - ./observability/grafana/dashboards:/var/lib/grafana/dashboards
    depends_on:
      - prometheus
```

## Kafka JMX port on the Kafka service

Kafka must expose JMX for the exporter to scrape:

```yaml
  kafka:
    environment:
      KAFKA_JMX_PORT: 9101
      KAFKA_JMX_HOSTNAME: kafka
      # ... other existing env vars
```

## Verifying observability setup

```bash
# Prometheus targets all UP
curl http://localhost:9090/targets

# Specific metric exists
curl 'http://localhost:9090/api/v1/query?query=kafka_consumer_group_lag' | python3 -m json.tool

# Grafana accessible (default: admin/admin)
curl http://localhost:3001

# Alert rules loaded
curl http://localhost:9090/api/v1/rules | python3 -m json.tool
```
