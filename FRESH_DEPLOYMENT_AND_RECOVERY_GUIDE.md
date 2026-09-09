# Confluent Platform on Kubernetes: Fresh Deployment, Architecture & Recovery Guide

This comprehensive guide details the architecture, root cause failure analysis, complete step-by-step fresh deployment runbook, and verification procedures for running a production-grade, 3-broker Highly Available (HA) Confluent Platform with RBAC, KRaft, Schema Registry, Kafka Connect, and Control Center on Amazon EKS (Auto Mode).

---

## 1. System Architecture & Topology

### Compute Infrastructure (Amazon EKS Auto Mode)
- **Kubernetes Control Plane**: v1.36
- **DNS Subsystem**: CoreDNS (`v1.14.3-eksbuild.14`) via `kube-dns` service (`10.100.0.10`)
- **Storage Subsystem**: AWS EBS CSI Driver (`standard` and `gp2` StorageClasses, `WaitForFirstConsumer`)
- **Worker Node Pool**: Karpenter dynamic autoscaling across multiple AWS EC2 instances (`c7i-flex.large`, Bottlerocket OS):
  - `i-0bbb150a3be878ea7` (Zone `us-east-1b`): Houses `confluent-operator`, `kafka-1`, `kafka-connect-0`, `schemaregistry-0`, `cloudflared-quick-tunnel`
  - `i-04501667b12bd4361` (Zone `us-east-1b`): Houses `kraftcontroller-0`, `kafka-0`, `kafka-2`, `schemaregistry-1`, `openldap`
  - `i-080bb58aae35726b4` (Zone `us-east-1b`): Houses `controlcenter-0`, `kafka-connect-1`

### Confluent Platform Topology
| Component | Replicas | Key Ports | Storage | Authentication / Security |
| :--- | :---: | :--- | :--- | :--- |
| **KRaft Controller** | 1 | `9074` (consensus) | 10Gi EBS (`standard`) | Isolated KRaft Controller |
| **Kafka Brokers** | 3 | `9071` (internal), `9072` (replication), `9073` (token/bearer), `9092` (nodePort/SASL_PLAIN), `8090` (MDS) | 10Gi EBS per broker | RBAC + MDS + OpenLDAP |
| **OpenLDAP** | 1 | `389` | In-memory / Ephemeral | Simple Bind (`cn=admin,dc=example,dc=com`) |
| **Schema Registry** | 2 | `8081` | Stateless | RBAC / Bearer Token via MDS |
| **Kafka Connect** | 2 | `8083` | Stateless (On-demand Datagen plugin) | RBAC / Bearer Token via MDS |
| **Control Center** | 1 | `9021` (NodePort `30021`) | 10Gi EBS (`standard`) | RBAC / Bearer Token via MDS |
| **Datagen Connector**| 1 | Connector Task (`datagen-users-source`) | Produces to `Product` topic | SASL PLAIN (`admin`) |

---

## 2. Root Cause Analysis & Engineering Solutions

During prior cluster lifecycles, the deployment experienced cascading restart loops and broker hung states. The root causes were systematically identified and resolved:

### Issue A: Total Cluster DNS Blackout (Missing CoreDNS)
- **Symptom**: Pods threw `dial tcp: lookup *.confluent.svc.cluster.local: i/o timeout`. `confluent-operator` failed rolebinding and metadata synchronization.
- **Root Cause**: The EKS cluster did not have the `coredns` managed addon deployed. `/etc/resolv.conf` pointed to nameserver `10.100.0.10`, but no service was bound to that cluster IP.
- **Resolution**: Created the EKS CoreDNS addon:
  ```bash
  aws eks create-addon --cluster-name Kube --addon-name coredns --region us-east-1
  ```
  DNS resolution across all cluster-local namespaces recovered immediately with sub-millisecond response times.

### Issue B: KRaft QuorumController Event Queue Contention (SBC Loop)
- **Symptom**: Kafka brokers stalled in `RECOVERY` state and never transitioned to `RUNNING`. KRaft controller logged:
  ```
  controller event queue overloaded. Timed out heartbeat from broker 0
  Handling event SbcConfigUpdateEvent-3
  Cluster metadata containing at least one unfenced broker not yet available, SBC startup delayed
  ```
- **Root Cause**: Confluent Self-Balancing Clusters (SBC) feature runs by default on Confluent Server. In KRaft mode before brokers are unfenced, SBC spins in a tight event-handling loop, flooding the single-threaded QuorumController event queue. Broker heartbeat RPCs timed out after 4500ms, preventing brokers from being unfenced.
- **Resolution**: Explicitly disabled Confluent Balancer in both `KRaftController` and `Kafka` specs:
  ```yaml
  configOverrides:
    server:
      - "confluent.balancer.enable=false"
  ```
  This reduced QuorumController request latency from >4500ms down to **1.9ms**, allowing all three brokers to unfence instantly.

### Issue C: Node Packing & Memory Starvation
- **Symptom**: Earlier pod specs requested only `512Mi` memory, while JVM heaps were configured to `-Xmx768m`.
- **Root Cause**: Because Kubernetes schedulers and Karpenter provision based strictly on **resource requests** (not limits), all workloads packed onto a single 4GB node (`i-0bbb150a3be878ea7`), consuming 96% memory and causing CPU context switching starvation.
- **Resolution**: Right-sized Kafka memory requests to `768Mi` (matching the heap limit). This caused Karpenter to detect memory capacity needs and spin up two additional worker nodes (`i-04501667b12bd4361` and `i-080bb58aae35726b4`), distributing pods into a true HA multi-node topology.

### Issue D: OpenLDAP Uninitialized Base DNs
- **Symptom**: Kafka brokers logged:
  ```
  javax.naming.NameNotFoundException: [LDAP: error code 32 - No Such Object]; remaining name 'ou=groups,dc=example,dc=com'
  ```
  and hung at `Waiting for all of the authorizer futures to be completed`, keeping readiness probe on port `9071` closed.
- **Root Cause**: The `openldap` pod was deployed without mounting the bootstrap LDIF ConfigMap. As a result, the LDAP directory contained only the root domain, missing `ou=users` and `ou=groups`.
- **Resolution**:
  1. Mounted `openldap-bootstrap-ldif` into `/container/service/slapd/assets/config/bootstrap/ldif/custom/custom.ldif` in [01-infrastructure.yaml](file:///Users/abhijithmh/Confluent-For-Kubernetees/01-infrastructure.yaml).
  2. Populated all user entries (`admin`, `dev_user`, `view_user`, `parvathi`) and groups (`kafka_admins`, `kafka_developers`). Authorizer futures completed immediately.

---

## 3. Step-by-Step Clean Deployment Runbook

If performing a clean re-installation from scratch, follow these ordered phases:

### Phase 1: Clean Teardown
```bash
# 1. Delete High-Level Ecosystem CRs
kubectl delete connector datagen-users-source -n confluent --ignore-not-found
kubectl delete confluentrolebinding --all -n confluent --ignore-not-found

# 2. Delete Ecosystem Deployments
kubectl delete -f 03-confluent-services.yaml --ignore-not-found

# 3. Delete Core Messaging
kubectl delete -f 02-kafka-cluster.yaml --ignore-not-found
kubectl delete -f 01-infrastructure.yaml --ignore-not-found

# 4. Purge Persistent Volume Claims (Erase EBS Volumes)
kubectl delete pvc -l app=kafka -n confluent
kubectl delete pvc -l app=kraftcontroller -n confluent
kubectl delete pvc -l app=controlcenter -n confluent

# 5. Ensure CoreDNS addon is active
aws eks describe-addon --cluster-name Kube --addon-name coredns --region us-east-1 --query "addon.status"
```

### Phase 2: Ordered Deployment
```bash
# Step 1: Secrets & Identity Management
kubectl apply -f 00-secrets.yaml
kubectl apply -f 01-ldap-configmap.yaml
kubectl apply -f 01-infrastructure.yaml

# Wait for OpenLDAP and KRaft Controller to be 1/1 Running
kubectl wait --for=condition=ready pod -l app=openldap -n confluent --timeout=180s
kubectl wait --for=condition=ready pod/kraftcontroller-0 -n confluent --timeout=180s

# Step 2: 3-Broker Kafka Cluster
kubectl apply -f 02-kafka-cluster.yaml

# Wait for all 3 Kafka brokers to reach 1/1 Running
kubectl wait --for=condition=ready pod/kafka-0 -n confluent --timeout=300s
kubectl wait --for=condition=ready pod/kafka-1 -n confluent --timeout=300s
kubectl wait --for=condition=ready pod/kafka-2 -n confluent --timeout=300s

# Step 3: Ecosystem Services & RBAC Rolebindings
kubectl apply -f 03-confluent-services.yaml
kubectl apply -f 05-rbac-rolebindings.yaml

# Wait for Schema Registry, Connect, and Control Center
kubectl wait --for=condition=ready pod -l app=schemaregistry -n confluent --timeout=300s
kubectl wait --for=condition=ready pod -l app=kafka-connect -n confluent --timeout=300s
kubectl wait --for=condition=ready pod/controlcenter-0 -n confluent --timeout=300s

# Step 4: Data Generator Connector & Target Topic
kubectl exec kafka-0 -c kafka -n confluent -- kafka-topics --bootstrap-server localhost:9071 --create --topic Product --partitions 3 --replication-factor 3 --if-not-exists
kubectl apply -f 06-datagen-connector.yaml
```

---

## 4. Verification & Health Check Procedures

### 1. Pod Health & Node Distribution
```bash
kubectl get pods -n confluent -o wide
```
**Expected**: All 12 pods `1/1` (or `4/4` for quick tunnel) `Running` with 0 restarts across multiple nodes.

### 2. Topic Replication & ISR
```bash
kubectl exec kafka-0 -c kafka -n confluent -- kafka-topics --bootstrap-server localhost:9071 --describe --topic _confluent-metadata-auth
```
**Expected**: `PartitionCount: 6`, `ReplicationFactor: 3`, `Isr: 2,1,0` with active leaders across all partitions.

### 3. Connector Production & Live Data Verification
```bash
# Check connector state
kubectl get connector datagen-users-source -n confluent

# Consume live generated messages
kubectl exec kafka-0 -c kafka -n confluent -- kafka-console-consumer --bootstrap-server localhost:9071 --topic Product --from-beginning --max-messages 3 --timeout-ms 10000
```
**Expected Output**:
```json
{"registertime":1516277685793,"userid":"User_8","regionid":"Region_4","gender":"MALE"}
{"registertime":1496960280696,"userid":"User_8","regionid":"Region_3","gender":"MALE"}
{"registertime":1507432746100,"userid":"User_6","regionid":"Region_5","gender":"MALE"}
```

### 4. Public Web UI Access
- **Control Center Public URL**: `https://devon-clothes-ear-section.trycloudflare.com`
- **Username**: `admin`
- **Password**: `adminpassword`
