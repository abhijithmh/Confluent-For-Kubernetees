# Confluent for Kubernetes (CFK) KRaft & MDS RBAC Deployment

This directory contains the production-ready Kubernetes manifests, LDAP schemas, secret properties, Python clients, and comprehensive deployment & debugging documentation for **Confluent Platform 7.6.0** with **KRaft** mode and **MDS RBAC (OpenLDAP)**.

---

## 📄 Documentation

- [`rbac_deployment_and_debugging_guide.md`](file:///home/ubuntu/Dev/CFK/rbac_deployment_and_debugging_guide.md): The complete architectural reference guide, LDAP schema breakdown, secrets deep-dive, and detailed debugging log analysis.

---

## 🛠️ Kubernetes Manifests & Configuration Files

| File | Description |
| :--- | :--- |
| [`kafka-cluster.yaml`](file:///home/ubuntu/Dev/CFK/kafka-cluster.yaml) | Confluent Kafka CRD with KRaft controller ref, embedded MDS, LDAP provider config, and JVM tuning |
| [`controlcenter.yaml`](file:///home/ubuntu/Dev/CFK/controlcenter.yaml) | Confluent Control Center UI CRD with MDS RBAC authentication & NodePort `30021` |
| [`kafka-rest-class.yaml`](file:///home/ubuntu/Dev/CFK/kafka-rest-class.yaml) | `KafkaRestClass` resource mapping Kafka cluster ID to MDS REST endpoint (`:8090`) |
| [`rbac-rolebindings.yaml`](file:///home/ubuntu/Dev/CFK/rbac-rolebindings.yaml) | `ConfluentRolebinding` resources (`SystemAdmin` & `DeveloperRead`) |
| [`openldap.yaml`](file:///home/ubuntu/Dev/CFK/openldap.yaml) | Lightweight OpenLDAP deployment and service (`:389`) |
| [`ldap-init.ldif`](file:///home/ubuntu/Dev/CFK/ldap-init.ldif) | LDIF data structure initializing `ou=users`, `ou=groups`, users (`admin`, `dev_user`, `view_user`), and groups |
| [`ldap-server-simple.txt`](file:///home/ubuntu/Dev/CFK/ldap-server-simple.txt) | Property file containing LDAP administrative bind credentials |
| [`mds-client-bearer.txt`](file:///home/ubuntu/Dev/CFK/mds-client-bearer.txt) | Property file containing MDS service client bearer credentials |
| [`cloudflare-tunnel-free.yaml`](file:///home/ubuntu/Dev/CFK/cloudflare-tunnel-free.yaml) | Cloudflare Quick Tunnel deployment exposing C3 UI (HTTP) & Kafka Broker (TCP) |
| [`kafka_client.py`](file:///home/ubuntu/Dev/CFK/kafka_client.py) | External Python Kafka client for producing/consuming via Cloudflare TCP tunnel |
| [`kraftcontroller.yaml`](file:///home/ubuntu/Dev/CFK/kraftcontroller.yaml) | KRaft Controller quorum deployment manifest |
| [`schemaregistry.yaml`](file:///home/ubuntu/Dev/CFK/schemaregistry.yaml) | Confluent Schema Registry CRD manifest |

---

## 🚀 One-Command Deployment

Apply all cluster resources:
```bash
kubectl apply -f /home/ubuntu/Dev/CFK/
```
