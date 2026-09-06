# Confluent Platform on Kubernetes (CFK) — Chronological Deployment Guide

This document provides a complete, step-by-step chronological reference for deploying **Confluent Platform on Kubernetes (CFK)** with **KRaft Controller**, **OpenLDAP Identity Provider**, **Metadata Service (MDS) RBAC**, **Schema Registry**, **Kafka Connect**, **Control Center**, **Kafka Exporter**, and **Cloudflare Tunnels** on **AWS EKS Auto Mode**.

---

## Architecture Overview

```
                          ┌──────────────────────────┐
                          │    OpenLDAP Server       │
                          │  (dc=example,dc=com)     │
                          └─────────────▲────────────┘
                                        │ LDAP Auth (Port 389)
                                        │
┌───────────────────────────────────────┴───────────────────────────────────────┐
│                          Kafka Broker (kafka-0)                               │
│  ┌─────────────────────────────────────────────────────────────────────────┐  │
│  │                Embedded MDS (Confluent Metadata Service)                │  │
│  │  - Signs & validates JWT bearer tokens via RSA 2048-bit Keypair          │  │
│  │  - Enforces RBAC permissions per Resource / Topic / Cluster             │  │
│  └────────────────────────────────────▲────────────────────────────────────┘  │
└───────────────────────────────────────┼───────────────────────────────────────┘
                                        │ REST / JWT Authentication
         ┌──────────────────────────────┼──────────────────────────────┐
         │                              │                              │
┌────────┴────────┐           ┌─────────┴─────────┐         ┌──────────┴──────────┐
│ Schema Registry │           │   Kafka Connect   │         │ Control Center (C3) │
│ (Port 8081)     │           │   (Port 8083)     │         │ (Port 9021)         │
└─────────────────┘           └───────────────────┘         └─────────────────────┘
```

---

## Chronological Step-by-Step Deployment Procedure

### Step 1: Storage Provisioning Fix for EKS Auto Mode

#### 1.1 Symptom & Initial Diagnosis
When deploying the KRaft controller (`kraftcontroller-0`), the pod remained stuck in `Pending` state:

```bash
kubectl get pod -n confluent
# Output: kraftcontroller-0 0/1 Pending
```

Inspecting pod events (`kubectl describe pod kraftcontroller-0 -n confluent`) revealed volume binding failure:
> `Failed to schedule pod, failed to validate pvc, provisioner is not supported (PersistentVolumeClaim=confluent/data0-kraftcontroller-0, Provisioner=ebs.csi.aws.com, StorageClass=gp2)`

#### 1.2 Root Cause Analysis
- The cluster runs on **AWS EKS Auto Mode** (`v1.36`).
- The default `gp2` StorageClass in Kubernetes was configured with `provisioner: kubernetes.io/aws-ebs` (the legacy in-tree driver), which is unsupported on Kubernetes 1.31+ and EKS Auto Mode.
- Inspecting node metadata (`kubectl get node <node-name> -o yaml`) confirmed that EKS Auto Mode registers nodes under the CSI provisioner `ebs.csi.eks.amazonaws.com`.

#### 1.3 Resolution & Manifest Updates
Updated `01-infrastructure.yaml` and `all-in-one/00-all-in-one.yaml` to declare StorageClasses using `ebs.csi.eks.amazonaws.com`:

```yaml
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: gp2
  annotations:
    storageclass.kubernetes.io/is-default-class: "true"
provisioner: ebs.csi.eks.amazonaws.com
volumeBindingMode: WaitForFirstConsumer
parameters:
  type: gp2
  fsType: ext4
---
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: standard
provisioner: ebs.csi.eks.amazonaws.com
volumeBindingMode: WaitForFirstConsumer
parameters:
  type: gp2
  fsType: ext4
```

Recreated the StorageClasses, deleted the pending PVC `data0-kraftcontroller-0`, and restarted `kraftcontroller-0`. The operator successfully allocated a 10Gi EBS volume (`pvc-6e28c1d9-0041-4a5c-b6bf-cf3c8273e8bd`) and `kraftcontroller-0` reached `1/1 Running`.

---

### Step 2: OpenLDAP Deployment & Schema Seeding

#### 2.1 Deploy OpenLDAP
Applied `01-infrastructure.yaml` to create the OpenLDAP Deployment and Service (`openldap.confluent.svc.cluster.local:389`).

#### 2.2 Apply LDIF ConfigMap & Seed Directory
Applied `01-ldap-configmap.yaml` and executed `ldapadd` inside the `openldap` container to seed organizational units, users, and groups:

```bash
kubectl apply -f 01-ldap-configmap.yaml
kubectl exec -i deployment/openldap -n confluent -- \
  ldapadd -x -D "cn=admin,dc=example,dc=com" -w adminpassword < ldap-init.ldif
```

**Seeded LDAP Structure**:
- **OUs**: `ou=users,dc=example,dc=com`, `ou=groups,dc=example,dc=com`
- **User Entries**:
  - `admin` (Password: `adminpassword`) — Confluent System Administrator
  - `dev_user` (Password: `devpassword`) — Application Developer
  - `view_user` (Password: `viewpassword`) — Read-only Viewer
- **Group Entries**:
  - `kafka_admins` (Member: `cn=admin,ou=users,dc=example,dc=com`)
  - `kafka_developers` (Member: `cn=dev_user,ou=users,dc=example,dc=com`)

---

### Step 3: RSA Token Keypair & Secrets Provisioning

Confluent MDS and ecosystem components require 3 Kubernetes secrets in namespace `confluent`:

#### 3.1 Generate RSA 2048-Bit Key Pair (`mds-token-pem`)
MDS uses an asymmetric RSA key pair to sign and verify JWT tokens.

```bash
# Generate private and public RSA keys
openssl genrsa -out mdsTokenKeyPair.pem 2048
openssl rsa -in mdsTokenKeyPair.pem -pubout -out mdsPublicKey.pem

# Create Kubernetes Secret
kubectl create secret generic mds-token-pem -n confluent \
  --from-file=mdsTokenKeyPair.pem=mdsTokenKeyPair.pem \
  --from-file=mdsPublicKey.pem=mdsPublicKey.pem
```

#### 3.2 Create LDAP Bind Credentials Secret (`ldap-bind-creds`)
Supplies credentials for MDS to perform simple LDAP binds during user lookup and group resolution.

```bash
# Create secret with standard file and alias key
kubectl create secret generic ldap-bind-creds -n confluent \
  --from-file=ldap-server-simple.txt=ldap-server-simple.txt \
  --from-file=ldap.txt=ldap-server-simple.txt
```

*File Content (`ldap-server-simple.txt`)*:
```properties
user=cn=admin,dc=example,dc=com
username=cn=admin,dc=example,dc=com
password=adminpassword
```

#### 3.3 Create MDS Client Credentials Secret (`mds-client-creds`)
Provides credentials for internal components (Control Center, Schema Registry, Connect, CFK Operator) to authenticate with MDS.

```bash
# Create secret with multi-alias filenames expected by CFK components
kubectl create secret generic mds-client-creds -n confluent \
  --from-file=mds-client-bearer.txt=mds-client-bearer.txt \
  --from-file=bearer.txt=mds-client-bearer.txt \
  --from-file=connect-client-bearer.txt=mds-client-bearer.txt \
  --from-file=schemaregistry-client-bearer.txt=mds-client-bearer.txt \
  --from-file=c3-client-bearer.txt=mds-client-bearer.txt
```

*File Content (`mds-client-bearer.txt`)*:
```properties
username=admin
password=adminpassword
```

---

### Step 4: External Access IP Alignment

Discovered that `02-kafka-cluster.yaml` and `all-in-one/00-all-in-one.yaml` referenced a stale public IP (`32.196.166.41`). Updated both manifests to match the active node external IP (`18.209.49.81`):

```yaml
  listeners:
    external:
      externalAccess:
        type: nodePort
        nodePort:
          host: 18.209.49.81
          nodePortOffset: 30022
```

and server override:
```yaml
  configOverrides:
    server:
      - "advertised.listeners=EXTERNAL://18.209.49.81:30023,INTERNAL://kafka-0.kafka.confluent.svc.cluster.local:9071,REPLICATION://kafka-0.kafka.confluent.svc.cluster.local:9072,TOKEN://kafka-0.kafka.confluent.svc.cluster.local:9073"
```

---

### Step 5: Kafka Broker Cluster Deployment & Compute Autoscaling

#### 5.1 Apply Kafka Cluster Manifest
Applied `02-kafka-cluster.yaml`:

```bash
kubectl apply -f 02-kafka-cluster.yaml
```

#### 5.2 Dynamic Compute Autoscaling
When `kafka-0` scheduled, EKS Auto Mode detected increased resource demands and automatically provisioned a new EC2 worker node (`i-09535ebb873c9754c`).

- Persistent volume `data0-kafka-0` (10Gi) was created and bound (`pvc-ddaccdff-9a50-434d-be25-e8c33bb1b0bb`).
- Pod `kafka-0` initialized MDS, connected to OpenLDAP (`ldap://openldap:389`), started Kafka Server, and reached `1/1 Running`.

---

### Step 6: Ecosystem Services & Monitoring Deployment

#### 6.1 Apply Ecosystem Services Manifest (`03-confluent-services.yaml`)
Deployed Schema Registry, Kafka Connect (with on-demand `kafka-connect-datagen` plugin build), and Control Center:

```bash
kubectl apply -f 03-confluent-services.yaml
```

- **Schema Registry** (`schemaregistry-0`): Authenticated via MDS, validated schemas, and reached `1/1 Running`.
- **Kafka Connect** (`kafka-connect-0`): Downloaded `confluentinc/kafka-connect-datagen:0.6.4`, scanned plugins, and reached `Running`.
- **Control Center** (`controlcenter-0`): Initialized monitoring streams, rebalanced internal topics, and reached `1/1 Running`.

#### 6.2 Apply Monitoring & Access Manifest (`04-monitoring-and-access.yaml`)
Deployed Kafka Exporter and Cloudflare Quick Tunnel:

```bash
kubectl apply -f 04-monitoring-and-access.yaml
```

- **`kafka-exporter`**: Connected to Kafka broker on port 9092 for metrics collection (`1/1 Running`).
- **`cloudflared-quick-tunnel`**: Started 4 tunnel replicas (`4/4 Running`).

---

### Step 7: RBAC RoleBindings & Datagen Connector

#### 7.1 Apply Confluent RoleBindings (`05-rbac-rolebindings.yaml`)
Applied RBAC definitions to bind OpenLDAP users and groups to Confluent resources via MDS:

```bash
kubectl apply -f 05-rbac-rolebindings.yaml
```

**Created RoleBindings**:
- `admin-system-admin`: Binds `User:admin` to `SystemAdmin` cluster-wide.
- `dev-user-developer-read`: Binds `User:dev_user` to `DeveloperRead` on topic resources.
- `admin-connect-all-connectors`: Binds `User:admin` to `ResourceOwner` for Connectors.
- `admin-connect-system-admin`: Binds `User:admin` to `SystemAdmin` for Connect.
- `admin-schemaregistry-system-admin`: Binds `User:admin` to `SystemAdmin` for Schema Registry.

#### 7.2 Apply Datagen Connector (`06-datagen-connector.yaml`)
Applied declarative connector definition:

```bash
kubectl apply -f 06-datagen-connector.yaml
```

---

## Final Verification & Cluster State

### Pod Status Summary

```bash
kubectl get pod -n confluent
```

```
NAME                                        READY   STATUS    RESTARTS   AGE
cloudflared-quick-tunnel-75c64bdd96-ptjnr   4/4     Running   0          13m
confluent-operator-dfcdc5499-4gf79          1/1     Running   0          23m
controlcenter-0                             1/1     Running   0          8m12s
kafka-0                                     1/1     Running   0          5m12s
kafka-connect-0                             1/1     Running   0          8m12s
kafka-exporter-5f8f67675c-4nx5b             1/1     Running   0          6m13s
kraftcontroller-0                           1/1     Running   0          20m
openldap-db8dd8f49-h27p9                    1/1     Running   0          23m
schemaregistry-0                            1/1     Running   0          8m12s
```

### Confluent RoleBindings Summary

```bash
kubectl get confluentrolebinding -n confluent
```

```
NAME                                  STATUS    PRINCIPAL       ROLE            AGE
admin-connect-all-connectors          CREATED   User:admin      ResourceOwner   13m
admin-connect-system-admin            CREATED   User:admin      SystemAdmin     13m
admin-schemaregistry-system-admin     CREATED   User:admin      SystemAdmin     13m
admin-system-admin                    CREATED   User:admin      SystemAdmin     13m
dev-user-developer-read               CREATED   User:dev_user   DeveloperRead   13m
internal-controlcenter-0              CREATED   User:admin      SystemAdmin     8m
internal-kafka-connect-0              CREATED   User:admin      SecurityAdmin   8m
internal-schemaregistry-0             CREATED   User:admin      SecurityAdmin   8m
```

---

## Key Troubleshooting & Technical Learnings

1. **EKS Auto Mode Storage Provisioner**: EKS Auto Mode requires `provisioner: ebs.csi.eks.amazonaws.com` (not legacy `kubernetes.io/aws-ebs` or `ebs.csi.aws.com`).
2. **Secrets Multi-Filename Aliasing**: CFK components look for specific credential file basenames inside mounted secrets (`bearer.txt`, `schemaregistry-client-bearer.txt`, `connect-client-bearer.txt`, `c3-client-bearer.txt`, `ldap.txt`). Including all alias names in secret definitions prevents `SecretRefIssue` errors.
3. **OpenLDAP Simple Bind Formatting**: LDAP bind DNs and passwords in `ldap-server-simple.txt` must use Unix LF line endings without Windows CR (`\r`) to avoid JNDI invalid DN exceptions.
