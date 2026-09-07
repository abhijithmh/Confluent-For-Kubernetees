# Confluent for Kubernetes (CFK) KRaft & MDS RBAC Deployment

This directory contains the production-ready Kubernetes manifests, LDAP schemas, secret properties, Python clients, and comprehensive deployment & debugging documentation for **Confluent Platform 7.6.0** with **KRaft** mode and **MDS RBAC (OpenLDAP)**.

---

## 📄 Documentation

- [`rbac_deployment_and_debugging_guide.md`](file:///home/ubuntu/Dev/CFK/rbac_deployment_and_debugging_guide.md): The complete architectural reference guide, LDAP schema breakdown, secrets deep-dive, and detailed debugging log analysis.

---

## 🛠️ Kubernetes Manifests & Configuration Files

| File | Description |
| :--- | :--- |
| [`00-secrets.yaml`](file:///Users/abhijithmh/Confluent-For-Kubernetees/00-secrets.yaml) | Declarative Kubernetes Secrets for MDS RSA keypair, LDAP bind creds, inter-component bearer tokens, and SASL/PLAIN user maps |
| [`01-infrastructure.yaml`](file:///Users/abhijithmh/Confluent-For-Kubernetees/01-infrastructure.yaml) | Core infrastructure: EBS StorageClasses (`gp2`, `standard`), OpenLDAP server, and KRaft Controller quorum |
| [`01-ldap-configmap.yaml`](file:///Users/abhijithmh/Confluent-For-Kubernetees/01-ldap-configmap.yaml) | ConfigMap containing LDIF dataset (`ou=users`, `ou=groups`, users `admin`, `dev_user`, `parvathi`) |
| [`02-kafka-cluster.yaml`](file:///Users/abhijithmh/Confluent-For-Kubernetees/02-kafka-cluster.yaml) | Confluent Kafka CRD with KRaft quorum ref, MDS RBAC enabled, LDAP provider, SASL/PLAIN external listener, and JVM tuning |
| [`03-confluent-services.yaml`](file:///Users/abhijithmh/Confluent-For-Kubernetees/03-confluent-services.yaml) | Confluent Schema Registry, Kafka Connect, and Control Center UI CRDs with MDS RBAC authentication |
| [`04-monitoring-and-access.yaml`](file:///Users/abhijithmh/Confluent-For-Kubernetees/04-monitoring-and-access.yaml) | Prometheus Kafka Exporter deployment and Cloudflare Quick Tunnel service |
| [`05-rbac-rolebindings.yaml`](file:///Users/abhijithmh/Confluent-For-Kubernetees/05-rbac-rolebindings.yaml) | `ConfluentRolebinding` CRDs for `SystemAdmin` and `DeveloperRead` roles |
| [`06-datagen-connector.yaml`](file:///Users/abhijithmh/Confluent-For-Kubernetees/06-datagen-connector.yaml) | `Connector` CRD generating mock orders stream into Kafka topic |
| [`all-in-one/00-all-in-one.yaml`](file:///Users/abhijithmh/Confluent-For-Kubernetees/all-in-one/00-all-in-one.yaml) | Single consolidated manifest containing all secrets, infrastructure, services, monitoring, and connectors |

---

## 🚀 Deployment Instructions

Apply all cluster resources declaratively:
```bash
# Option 1: Apply individual ordered manifests
kubectl apply -f 00-secrets.yaml
kubectl apply -f 01-ldap-configmap.yaml
kubectl apply -f 01-infrastructure.yaml
kubectl apply -f 02-kafka-cluster.yaml
kubectl apply -f 03-confluent-services.yaml
kubectl apply -f 04-monitoring-and-access.yaml
kubectl apply -f 05-rbac-rolebindings.yaml
kubectl apply -f 06-datagen-connector.yaml

# Option 2: Apply single consolidated manifest
kubectl apply -f all-in-one/00-all-in-one.yaml
```
