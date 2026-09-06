# Confluent Platform on Kubernetes: External Connectivity & iptables Guide

## 1. Architecture & Networking Overview

Connecting external applications (such as local developer laptops or remote microservices) to a **Confluent For Kubernetes (CFK)** cluster running inside **Minikube** on an **AWS EC2** instance involves traversing three distinct network layers:

```
[ External Client / Laptop ]
            │
            ▼ (Public IP: 32.196.166.41)
┌───────────────────────────────────────────────────────────────────────────┐
│ Layer 1: AWS EC2 Security Group (Inbound Rules: 30021, 30022, 30023, 9092)│
└─────────────────────────────────────┬─────────────────────────────────────┘
                                      │
┌─────────────────────────────────────▼─────────────────────────────────────┐
│ Layer 2: Host Linux Kernel & iptables NAT (EC2 Host: 172.31.11.23)         │
│  - PREROUTING & OUTPUT (DNAT: 32.196.166.41:30022 -> 192.168.49.2:30022)   │
│  - DOCKER-USER (ACCEPT rules bypassing Docker bridge DROP policy)          │
└─────────────────────────────────────┬─────────────────────────────────────┘
                                      │
┌─────────────────────────────────────▼─────────────────────────────────────┐
│ Layer 3: Minikube Node & Pod Network (Minikube IP: 192.168.49.2)         │
│  - NodePort Services (30021, 30022, 30023, 30938)                         │
│  - Kafka Broker (advertised.listeners=EXTERNAL://32.196.166.41:30023)     │
└───────────────────────────────────────────────────────────────────────────┘
```

---

## 2. Port Mapping Matrix

| Service | Internal Port | Minikube NodePort | Host Port (DNAT) | AWS Security Group | External Access URL / Endpoint |
|---|---|---|---|---|---|
| **Control Center UI** | `9021` | `30021` | `30021` | `30021` | `http://32.196.166.41:30021` |
| **Kafka Bootstrap** | `9092` | `30022` | `30022` & `9092` | `30022` & `9092` | `32.196.166.41:30022` |
| **Kafka Broker 0** | `9092` | `30023` | `30023` | `30023` | `32.196.166.41:30023` |
| **Kafka Exporter** | `9308` | `30938` | `30938` | `30938` | `http://32.196.166.41:30938` |
| **OpenLDAP** | `389` | N/A (Internal) | N/A | N/A | `openldap.confluent.svc.cluster.local:389` |
| **Schema Registry** | `8081` | N/A (Internal) | N/A | N/A | `schemaregistry.confluent.svc.cluster.local:8081` |
| **Kafka Connect** | `8083` | N/A (Internal) | N/A | N/A | `kafka-connect.confluent.svc.cluster.local:8083` |

---

## 3. Detailed Explanation of iptables Setup

Because Minikube runs inside a Docker container (`kicbase`) on network `192.168.49.0/24`, NodePorts are created on the Minikube virtual interface (`192.168.49.2`) rather than directly on the host's public network interface (`eth0` / `32.196.166.41`).

Without custom `iptables` rules, incoming traffic on public IP `32.196.166.41` would either be rejected by the host or dropped by Docker's default bridge isolation policies.

### A. Destination NAT (`PREROUTING` Chain - `nat` Table)
Translates incoming packets arriving from external network interfaces (`eth0` / public IP) to target the internal Minikube IP (`192.168.49.2`).

```bash
# Forward external requests to Minikube NodePorts
sudo iptables -t nat -I PREROUTING 1 -p tcp --dport 30021 -j DNAT --to-destination 192.168.49.2:30021
sudo iptables -t nat -I PREROUTING 1 -p tcp --dport 30022 -j DNAT --to-destination 192.168.49.2:30022
sudo iptables -t nat -I PREROUTING 1 -p tcp --dport 30023 -j DNAT --to-destination 192.168.49.2:30023
sudo iptables -t nat -I PREROUTING 1 -p tcp --dport 9092  -j DNAT --to-destination 192.168.49.2:30022
sudo iptables -t nat -I PREROUTING 1 -p tcp --dport 30938 -j DNAT --to-destination 192.168.49.2:30938
```

### B. Local Host NAT (`OUTPUT` Chain - `nat` Table)
Translates packets generated **locally on the EC2 host** when testing against public IP `32.196.166.41` (packets generated locally bypass the `PREROUTING` chain and enter the `OUTPUT` chain).

```bash
# Forward local host requests targeting public IP to Minikube
sudo iptables -t nat -A OUTPUT -p tcp -d 32.196.166.41 --dport 30021 -j DNAT --to-destination 192.168.49.2:30021
sudo iptables -t nat -A OUTPUT -p tcp -d 32.196.166.41 --dport 30022 -j DNAT --to-destination 192.168.49.2:30022
sudo iptables -t nat -A OUTPUT -p tcp -d 32.196.166.41 --dport 30023 -j DNAT --to-destination 192.168.49.2:30023
sudo iptables -t nat -A OUTPUT -p tcp -d 32.196.166.41 --dport 9092  -j DNAT --to-destination 192.168.49.2:30022
sudo iptables -t nat -A OUTPUT -p tcp -d 32.196.166.41 --dport 30938 -j DNAT --to-destination 192.168.49.2:30938
```

### C. Docker Bridge Filtering (`DOCKER-USER` Chain - `filter` Table)
Docker automatically manages the `FORWARD` chain and places a `DROP` policy on non-docker bridge traffic. The `DOCKER-USER` chain is the official Docker mechanism to inject explicit `ACCEPT` rules for exposed ports **before** Docker's default drop filters execute.

```bash
# Allow external packets arriving at the host to cross into the Minikube bridge
sudo iptables -I DOCKER-USER 1 -p tcp --dport 30021 -j ACCEPT
sudo iptables -I DOCKER-USER 1 -p tcp --dport 30022 -j ACCEPT
sudo iptables -I DOCKER-USER 1 -p tcp --dport 30023 -j ACCEPT
sudo iptables -I DOCKER-USER 1 -p tcp --dport 9092  -j ACCEPT
sudo iptables -I DOCKER-USER 1 -p tcp --dport 30938 -j ACCEPT
```

### D. Source NAT & Masquerading (`POSTROUTING` Chain - `nat` Table)
Ensures return packets from Minikube pods are correctly rewritten with the host IP so external clients receive response packets from `32.196.166.41`.

```bash
sudo iptables -t nat -A POSTROUTING -p tcp --dst 192.168.49.2 -j MASQUERADE
```

---

## 4. Kafka Advertised Listener Protocol Explained

Connecting to Kafka requires a **two-step protocol handshake**:

1. **Bootstrap Phase**:
   - Client connects to `BOOTSTRAP_SERVERS` (`32.196.166.41:30022` or `32.196.166.41:9092`).
   - Kafka returns Metadata containing broker IDs and advertised endpoints.

2. **Produce/Consume Phase**:
   - Kafka returns `advertised.listeners=EXTERNAL://32.196.166.41:30023`.
   - Client opens a new TCP connection directly to `32.196.166.41:30023` to produce/consume data.

If `30023` (Broker 0) is blocked in firewall or AWS Security Group, the initial bootstrap succeeds, but `producer.send()` fails after 60 seconds with `KafkaTimeoutError: Failed to update metadata`.

### CFK Spec Configuration (`02-kafka-cluster.yaml`)

```yaml
spec:
  listeners:
    external:
      externalAccess:
        type: nodePort
        nodePort:
          host: 32.196.166.41
          nodePortOffset: 30022
  configOverrides:
    server:
      - "advertised.listeners=EXTERNAL://32.196.166.41:30023,INTERNAL://kafka-0.kafka.confluent.svc.cluster.local:9071,REPLICATION://kafka-0.kafka.confluent.svc.cluster.local:9072,TOKEN://kafka-0.kafka.confluent.svc.cluster.local:9073"
```

---

## 5. Persistence & Verification Commands

### Save Rules Persistently
To ensure iptables rules survive host reboots:
```bash
sudo mkdir -p /etc/iptables
sudo iptables-save | sudo tee /etc/iptables/rules.v4 > /dev/null
```

### Inspect Active Rules
```bash
# View PREROUTING NAT table
sudo iptables -w 5 -t nat -L PREROUTING -n -v

# View DOCKER-USER filtering chain
sudo iptables -w 5 -L DOCKER-USER -n -v
```

### External Client Verification (`kcat`)
```bash
# Test Metadata lookup from remote machine
kcat -b 32.196.166.41:30022 -L

# Test Producing a record from remote machine
echo "hello world" | kcat -b 32.196.166.41:30022 -t test-topic -P

# Test Consuming records from remote machine
kcat -b 32.196.166.41:30022 -t test-topic -C -o beginning -e
```
