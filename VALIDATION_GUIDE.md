# Guia de Validação — doc-validator-relay

Passo a passo completo para validar o serviço Go em uma nova máquina,
desde a preparação do ambiente até a confirmação de publicação no RabbitMQ.

---

## Pré-requisitos

- Ubuntu 22.04
- Docker e Docker Compose v2 instalados
- Go 1.25 instalado
- Projetos clonados:
  - `doc_validator` (Rails) — com o `docker-compose-without-rabbitmq.yml` na raiz
  - `doc-validator-relay` (Go)

### Instalar Go 1.25 (se necessário)

```bash
curl -OL https://go.dev/dl/go1.25.0.linux-amd64.tar.gz
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf go1.25.0.linux-amd64.tar.gz
echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
source ~/.bashrc
go version
# go version go1.25.0 linux/amd64
```

---

## Parte 1 — Preparar o projeto Go

Na pasta `doc-validator-relay`:

```bash
# 1. Copiar e preencher as variáveis de ambiente
cp .env.example .env
```

Editar o `.env` com as credenciais:
```dotenv
POSTGRES_USER=doc_validator
POSTGRES_PASSWORD=senha123
RABBITMQ_USER=doc_validator
RABBITMQ_PASSWORD=senha123
```

```bash
# 2. Baixar dependências e gerar go.sum
go mod tidy

# 3. Verificar que compila
go build ./...

# 4. Rodar os testes unitários (não precisa de Docker)
go test ./...
```

Resultado esperado dos testes:
```
?     github.com/doc-validator/relay/cmd/relay          [no test files]
?     github.com/doc-validator/relay/internal/config    [no test files]
?     github.com/doc-validator/relay/internal/db        [no test files]
ok    github.com/doc-validator/relay/internal/outbox    ~3s
ok    github.com/doc-validator/relay/internal/outbox/retry
?     github.com/doc-validator/relay/internal/publisher [no test files]
```

> Os pacotes `[no test files]` são esperados — db e publisher requerem
> infraestrutura real e são cobertos pelo teste de ponta a ponta abaixo.

---

## Parte 2 — Subir o Rails e gerar os dados de teste

### 2.1 Subir Postgres e MinIO

Na pasta `doc_validator` (Rails):

```bash
docker compose -f docker-compose-without-rabbitmq.yml -p doc_validator up db minio -d
```

Aguardar o Postgres ficar healthy:
```bash
docker compose -f docker-compose-without-rabbitmq.yml -p doc_validator ps
# db deve mostrar "healthy"
```

### 2.2 Criar o bucket no MinIO

```bash
docker compose -f docker-compose-without-rabbitmq.yml -p doc_validator run --rm web rails runner "
  require 'aws-sdk-s3'
  client = Aws::S3::Client.new(
    endpoint: ENV['MINIO_ENDPOINT'],
    access_key_id: ENV['MINIO_ACCESS_KEY'],
    secret_access_key: ENV['MINIO_SECRET_KEY'],
    region: ENV['MINIO_REGION'],
    force_path_style: true
  )
  client.create_bucket(bucket: ENV['MINIO_BUCKET']) rescue nil
  puts 'Bucket OK'
"
```

### 2.3 Rodar as migrations

```bash
docker compose -f docker-compose-without-rabbitmq.yml -p doc_validator run --rm web rails db:migrate
```

> Os warnings de conexão ao RabbitMQ são esperados e podem ser ignorados —
> o RabbitMQ não existe neste compose propositalmente.

### 2.4 Rodar o seed de teste

Confirmar que o arquivo está em `db/seeds/relay_test.rb` na raiz do projeto Rails.

```bash
docker compose -f docker-compose-without-rabbitmq.yml -p doc_validator run --rm web rails runner db/seeds/relay_test.rb
```

Resultado esperado:
```
==> Limpando dados anteriores do seed...
==> Criando empresa...
==> Criando usuário admin...
==> Criando candidato...
==> Criando documentos e outbox_events...
    [OK] Curriculo → outbox_event criado (document_id: 1)
    [OK] Diploma 1 → outbox_event criado (document_id: 2)
    [OK] Diploma 2 → outbox_event criado (document_id: 3)

==> Seed concluído!
    OutboxEvents pendentes:
      id: 1  event_type: document.created  status: pending
      id: 2  event_type: document.created  status: pending
      id: 3  event_type: document.created  status: pending
```

### 2.5 Confirmar eventos no banco

```bash
docker exec -it doc_validator-db-1 psql -U doc_validator -d doc_validator_development \
  -c "SELECT id, event_type, status, retry_count, infrastructure_retry_count FROM outbox_events ORDER BY id;"
```

Esperado: 3 linhas com `status = 0` e todos os contadores zerados.

Se algum status estiver diferente de 0, executar o reset (arquivo na raiz do projeto Rails):

```bash
docker exec -i doc_validator-db-1 psql -U doc_validator -d doc_validator_development \
  < relay_test_reset.sql
```

---

## Parte 3 — Subir o relay Go

Na pasta `doc-validator-relay`:

```bash
docker compose up --build
```

### Logs esperados na inicialização

```
relay-1  | INFO  postgres connected
relay-1  | INFO  exchange ready       {"exchange": "doc_validator.events"}
relay-1  | INFO  queue ready          {"queue": "doc_validator.main_queue"}
relay-1  | INFO  queue ready          {"queue": "doc_validator.retry_queue"}
relay-1  | INFO  queue ready          {"queue": "doc_validator.dead_letter_queue"}
relay-1  | INFO  binding ready        {"queue": "doc_validator.main_queue", ...}
relay-1  | INFO  binding ready        {"queue": "doc_validator.retry_queue", ...}
relay-1  | INFO  binding ready        {"queue": "doc_validator.dead_letter_queue", ...}
relay-1  | INFO  rabbitmq topology ready
relay-1  | INFO  rabbitmq publisher ready
relay-1  | INFO  poller started       {"batch_size": 100, "poll_interval": "3s", "worker_count": 10}
relay-1  | DEBUG claimed batch        {"count": 3}
relay-1  | INFO  event published      {"event_id": 1, "event_type": "document.created"}
relay-1  | INFO  event published      {"event_id": 2, "event_type": "document.created"}
relay-1  | INFO  event published      {"event_id": 3, "event_type": "document.created"}
```

### Confirmar publicação no banco

```bash
docker exec -it doc_validator-db-1 psql -U doc_validator -d doc_validator_development \
  -c "SELECT id, event_type, status FROM outbox_events ORDER BY id;"
```

Esperado: todos com `status = 1` (published). ✅

### Confirmar no RabbitMQ (opcional)

Acessar http://localhost:15672 com as credenciais do `.env`.
Em **Queues → doc_validator.main_queue** verificar as mensagens entregues.

---

## Parte 4 — Teste de retry de infraestrutura (opcional)

Valida o comportamento quando o RabbitMQ fica indisponível.

### 4.1 Subir o relay em background

```bash
docker compose up -d
```

### 4.2 Derrubar o RabbitMQ

```bash
docker compose stop rabbitmq
```

### 4.3 Inserir um evento manualmente

```bash
docker exec -it doc_validator-db-1 psql -U doc_validator -d doc_validator_development -c "
INSERT INTO outbox_events (event_type, payload, status, retry_count, infrastructure_retry_count, created_at, updated_at)
VALUES (
  'document.created',
  '{\"event\":\"document.created\",\"timestamp\":\"2026-01-01T00:00:00Z\",\"data\":{\"document_id\":99,\"document_type\":\"curriculum\",\"candidate_id\":1,\"candidate_name\":\"João Silva\",\"company_id\":1,\"actor_email\":\"admin@relaytest.com\",\"reason\":null}}',
  0, 0, 0, NOW(), NOW()
);"
```

### 4.4 Acompanhar os logs

```bash
docker compose logs -f relay
```

Esperado: falhas de infraestrutura com `infrastructure_retry_count` incrementando.

### 4.5 Inspecionar o banco

```bash
docker exec -it doc_validator-db-1 psql -U doc_validator -d doc_validator_development \
  -c "SELECT id, status, infrastructure_retry_count, infrastructure_retry_after, failure_reason FROM outbox_events WHERE id = (SELECT MAX(id) FROM outbox_events);"
```

Esperado: `status = 0`, `failure_reason = infrastructure`, contador incrementando.

### 4.6 Restaurar o RabbitMQ

```bash
docker compose start rabbitmq
```

Aguardar o `infrastructure_retry_after` expirar (primeiro retry em 1 minuto).
O relay vai publicar automaticamente assim que o tempo passar.

### 4.7 Confirmar publicação

```bash
docker exec -it doc_validator-db-1 psql -U doc_validator -d doc_validator_development \
  -c "SELECT id, status, infrastructure_retry_count, failure_reason FROM outbox_events WHERE id = (SELECT MAX(id) FROM outbox_events);"
```

Esperado: `status = 1` (published). ✅

---

## Resumo dos comandos — ordem de execução

```bash
# ── Go (verificação local) ────────────────────────────────────────────────────
cd doc-validator-relay

cp .env.example .env          # preencher credenciais

go mod tidy

go build ./...

go test ./...

# ── Rails (dados de teste) ────────────────────────────────────────────────────
cd doc_validator

docker compose -f docker-compose-without-rabbitmq.yml -p doc_validator up db minio -d

docker compose -f docker-compose-without-rabbitmq.yml -p doc_validator run --rm web rails runner "require 'aws-sdk-s3'; Aws::S3::Client.new(endpoint: ENV['MINIO_ENDPOINT'], access_key_id: ENV['MINIO_ACCESS_KEY'], secret_access_key: ENV['MINIO_SECRET_KEY'], region: ENV['MINIO_REGION'], force_path_style: true).create_bucket(bucket: ENV['MINIO_BUCKET']) rescue nil; puts 'Bucket OK'"

docker compose -f docker-compose-without-rabbitmq.yml -p doc_validator run --rm web rails db:migrate

docker compose -f docker-compose-without-rabbitmq.yml -p doc_validator run --rm web rails runner db/seeds/relay_test.rb

# ── Go (relay) ────────────────────────────────────────────────────────────────
cd doc-validator-relay

docker compose up --build
```
