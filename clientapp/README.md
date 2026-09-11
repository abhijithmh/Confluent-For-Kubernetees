# Production Kafka Consumer & Producer Package for Kubernetes

A production-ready, modular, and cloud-native Python Kafka Consumer & Producer suite designed for **Confluent Platform on Kubernetes (CFK)** and standalone Kafka clusters.

It supports producing and consuming across:
- **External clients / laptops / microservices** over TCP NodePort / LoadBalancer (e.g., `32.198.108.214:30022`)
- **In-cluster workloads** via Kubernetes internal Service DNS (e.g., `kafka.confluent.svc.cluster.local:9092`)

---

## Features

- **12-Factor Configuration**: Fully configurable via environment variables, `ConfigMap`, and `Secret`.
- **Producer & Consumer Suite**: Includes both a resilient consumer and a high-performance continuous producer.
- **Kubernetes Native Probes**: Built-in HTTP health servers exposing `/healthz` (liveness) and `/readyz` (readiness) on ports `8080` (consumer) and `8081` (producer).
- **Graceful Shutdown**: Intercepts `SIGINT` and `SIGTERM`, flushes buffers, commits consumer offsets, and cleanly unregisters from consumer groups.
- **Safe Serialization / Deserialization**: Automatically parses and serializes JSON objects, UTF-8 strings, and handles raw bytes with error fallbacks.
- **Pluggable Architecture**: Customizable `BaseMessageHandler` for consumer sinks, and `BaseMessageGenerator` for custom producer data feeds.
- **Security**: Hardened Docker image running as a non-root user (`UID 10001`).

---

## Directory Structure

```
clientapp/
├── Dockerfile                  # Multi-command container with non-root security
├── pyproject.toml              # Build spec with 'kafka-consumer' and 'kafka-producer' CLI
├── requirements.txt            # Pinned runtime dependencies (including lz4 & zstandard)
├── k8s/                        # Production Kubernetes manifests
│   ├── base/                   # Shared cluster foundations
│   │   ├── namespace.yaml      # 'kafka' namespace definition
│   │   ├── secret.yaml         # Kafka SASL credentials (shared)
│   │   └── kustomization.yaml  # Base kustomization
│   ├── consumer/               # Kafka Consumer resources
│   │   ├── configmap.yaml      # Consumer configuration (topics, group, offset reset)
│   │   ├── deployment.yaml     # Consumer Deployment (probe port 8080)
│   │   └── kustomization.yaml  # Consumer kustomization
│   ├── producer/               # Kafka Producer resources
│   │   ├── configmap.yaml      # Producer configuration (interval, topic, acks)
│   │   ├── deployment.yaml     # Producer Deployment (probe port 8081)
│   │   └── kustomization.yaml  # Producer kustomization
│   └── kustomization.yaml      # Root composite (deploys base + consumer + producer)
├── src/
│   ├── kafka_consumer/         # Modular consumer package
│   │   ├── config.py           # Consumer configuration
│   │   ├── consumer.py         # Main consumer loop
│   │   ├── deserializer.py     # Safe JSON/text deserializer
│   │   ├── handlers.py         # Message handler architecture
│   │   ├── health.py           # Embedded health server (port 8080)
│   │   └── cli.py              # CLI entry point
│   └── kafka_producer/         # Modular producer package
│       ├── config.py           # Producer configuration
│       ├── producer.py         # Main producer loop with async delivery callbacks
│       ├── serializer.py       # Safe JSON/string serializer
│       ├── generators.py       # Pluggable message generators (ProductDataGenerator)
│       ├── health.py           # Embedded health server (port 8081)
│       └── cli.py              # CLI entry point
└── tests/                      # 30 automated unit tests
```

---

## Configuration Reference

All settings can be provided via environment variables or Kubernetes `ConfigMap`/`Secret`:

| Variable | Default | Description |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Comma-separated list of bootstrap servers (e.g. `32.198.108.214:30022` or `kafka.confluent.svc.cluster.local:9092`) |
| `KAFKA_TOPICS` | `Product` | Target topic(s), comma-separated |
| `KAFKA_GROUP_ID` | `my-python-consumer-group` | Kafka Consumer Group ID |
| `KAFKA_SECURITY_PROTOCOL` | `SASL_PLAINTEXT` | Protocol: `SASL_PLAINTEXT`, `PLAINTEXT`, `SASL_SSL`, `SSL` |
| `KAFKA_SASL_MECHANISM` | `PLAIN` | SASL mechanism: `PLAIN`, `SCRAM-SHA-256`, `SCRAM-SHA-512` |
| `KAFKA_USERNAME` | `admin` | SASL plain username |
| `KAFKA_PASSWORD` | - | SASL plain password |
| `KAFKA_AUTO_OFFSET_RESET` | `earliest` | Offset reset strategy (`earliest` or `latest`) |
| `KAFKA_ENABLE_AUTO_COMMIT`| `true` | Enable offset auto commit (`true`/`false`) |
| `HEALTH_PORT` | `8080` | Port for `/healthz` and `/readyz` probes |
| `HEALTH_ENABLED` | `true` | Enable or disable embedded health HTTP server |
| `LOG_LEVEL` | `INFO` | Logging level (`DEBUG`, `INFO`, `WARNING`, `ERROR`) |

---

## Quick Start (Local Run)

### 1. Install Dependencies
```bash
pip install -r requirements.txt
pip install -e .
```

### 2. Run Consumer
```bash
export KAFKA_BOOTSTRAP_SERVERS="32.198.108.214:30022"
export KAFKA_TOPICS="Product"
export KAFKA_GROUP_ID="my-python-consumer-group"
export KAFKA_USERNAME="admin"
export KAFKA_PASSWORD="adminpassword"
export KAFKA_SECURITY_PROTOCOL="SASL_PLAINTEXT"

# Run Consumer:
kafka-consumer
# or:
python3 -m kafka_consumer
```

### 3. Run Producer
In a separate terminal:
```bash
export KAFKA_BOOTSTRAP_SERVERS="32.198.108.214:30022"
export KAFKA_TOPIC="Product"
export KAFKA_USERNAME="admin"
export KAFKA_PASSWORD="adminpassword"
export KAFKA_SECURITY_PROTOCOL="SASL_PLAINTEXT"
export PRODUCE_INTERVAL_SECONDS="1.0"

# Run Producer:
kafka-producer
# or:
python3 -m kafka_producer
```

### 4. Check Embedded Health Servers
- **Consumer Health** (`http://localhost:8080`):
  ```bash
  curl http://localhost:8080/healthz   # {"status": "UP"}
  curl http://localhost:8080/readyz    # {"status": "READY"}
  curl http://localhost:8080/status    # Consumer diagnostics
  ```
- **Producer Health** (`http://localhost:8081`):
  ```bash
  curl http://localhost:8081/healthz   # {"status": "UP"}
  curl http://localhost:8081/readyz    # {"status": "READY"}
  curl http://localhost:8081/status    # Producer message counters
  ```

---

## Docker Usage

### Build the Image
```bash
docker build -t kafka-consumer:latest .
```

### Run Container
```bash
docker run --rm -it \
  -p 8080:8080 \
  -e KAFKA_BOOTSTRAP_SERVERS="32.198.108.214:30022" \
  -e KAFKA_TOPICS="Product" \
  -e KAFKA_USERNAME="admin" \
  -e KAFKA_PASSWORD="adminpassword" \
  kafka-consumer:latest
```

---

## Deploy to Kubernetes (AWS EKS & ECR)

Follow these steps to build the container image, push it to Amazon Elastic Container Registry (ECR), and deploy it to your Kubernetes cluster.

### Prerequisites

- **AWS CLI** configured (`aws configure` or SSO credentials)
- **Docker Desktop** / Docker Daemon running
- **kubectl** configured with your EKS cluster context:
  ```bash
  aws eks update-kubeconfig --region us-east-1 --name Kube
  ```

---

### Step 1: Set Environment Variables

Define your AWS Region, Account ID, and ECR Repository Name:

```bash
export AWS_REGION="us-east-1"
export AWS_ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
export ECR_REPO="kafka-consumer"
export IMAGE_TAG="latest"
export ECR_URI="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com/${ECR_REPO}:${IMAGE_TAG}"

echo "Target ECR Image: ${ECR_URI}"
```

---

### Step 2: Create ECR Repository & Authenticate Docker

1. **Create the ECR repository** (if not already created):
   ```bash
   aws ecr create-repository \
     --repository-name ${ECR_REPO} \
     --region ${AWS_REGION} || true
   ```

2. **Log in Docker to your ECR registry**:
   ```bash
   aws ecr get-login-password --region ${AWS_REGION} | \
     docker login --username AWS --password-stdin ${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com
   ```

---

### Step 3: Build and Push Container Image

> [!IMPORTANT]
> **Cross-Platform Compilation (`linux/amd64`)**:
> If building from an Apple Silicon Mac (`arm64`), you must target `--platform linux/amd64` to match standard AWS EKS Linux/Bottlerocket worker nodes.

Build and push in one command using Docker Buildx:

```bash
docker buildx build \
  --platform linux/amd64 \
  -t ${ECR_URI} \
  --push .
```

---

### Step 4: Configure Separated Kubernetes Manifests

The Kubernetes manifests are separated into modular directories:

1. **Shared Base (`k8s/base/`)**:
   - `namespace.yaml`: Defines `kafka` namespace.
   - `secret.yaml`: Shared SASL credentials (`KAFKA_USERNAME`, `KAFKA_PASSWORD`).

2. **Consumer (`k8s/consumer/`)**:
   - `configmap.yaml`: `KAFKA_TOPICS`, `KAFKA_GROUP_ID`, `KAFKA_AUTO_OFFSET_RESET`, `HEALTH_PORT=8080`.
   - `deployment.yaml`: Consumer pods, probes on `/healthz` & `/readyz` (port 8080).

3. **Producer (`k8s/producer/`)**:
   - `configmap.yaml`: `KAFKA_TOPIC`, `PRODUCE_INTERVAL_SECONDS`, `KAFKA_ACKS`, `HEALTH_PORT=8081`.
   - `deployment.yaml`: Producer pods, `command: ["kafka-producer"]`, probes on port 8081.

---

### Step 5: Deploy to Kubernetes

You can deploy the entire stack together, or deploy components individually:

#### Option A: Deploy All Components Together (Recommended)
```bash
kubectl apply -k k8s/
```

#### Option B: Deploy Individually
```bash
# 1. Deploy Namespace and Credentials:
kubectl apply -k k8s/base/

# 2. Deploy Consumer:
kubectl apply -k k8s/consumer/

# 3. Deploy Producer:
kubectl apply -k k8s/producer/
```

---

### Step 6: Verify Pod Health & Stream Logs

1. **Check Pod Status**:
   ```bash
   kubectl get pods -n kafka -l app.kubernetes.io/name=kafka-consumer
   ```
   *(Expected status: `1/1 Running` with readiness probe passed)*

2. **Stream Live Message Logs**:
   ```bash
   kubectl logs -f -n kafka -l app.kubernetes.io/name=kafka-consumer
   ```

3. **Scale Consumers (Parallel Partitions)**:
   Scale up consumer replicas to match partition count:
   ```bash
   kubectl scale deployment kafka-consumer -n kafka --replicas=3
   ```

4. **Restart / Trigger Rolling Update**:
   ```bash
   kubectl rollout restart deployment kafka-consumer -n kafka
   kubectl rollout status deployment kafka-consumer -n kafka
   ```

---

## Custom Message Handlers

To process messages with custom business logic (e.g. database persistence or calling an API), subclass `BaseMessageHandler`:

```python
from kafka_consumer import GenericKafkaConsumer, ConsumerConfig, BaseMessageHandler

class DatabaseSinkHandler(BaseMessageHandler):
    def handle(self, record):
        # record.value is already deserialized (e.g. Python dict if JSON)
        print(f"Saving product to DB: {record.value.get('name')}")
        # db.insert(record.value)

config = ConsumerConfig.from_env()
consumer = GenericKafkaConsumer(
    config=config,
    handler=DatabaseSinkHandler()
)
consumer.start()
```

---

## Running Unit Tests

Execute the automated test suite:
```bash
PYTHONPATH=src python3 -m unittest discover -s tests -v
```
