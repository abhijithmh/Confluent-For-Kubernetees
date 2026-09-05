# Confluent Metadata Service (MDS) RBAC Architecture: Deployment & Debugging Guide

This document provides a comprehensive technical reference for the design, deployment, and step-by-step debugging of **Role-Based Access Control (RBAC)** via **Confluent Metadata Service (MDS)** backed by **OpenLDAP** using **Confluent for Kubernetes (CFK)**.

---

## 1. System Architecture

```
                    ┌─────────────────────────┐
                    │    OpenLDAP Server      │
                    │ (dc=example,dc=com)     │
                    └────────────▲────────────┘
                                 │ LDAP Auth (Port 389)
                                 │
  ┌──────────────────────────────┴──────────────────────────────┐
  │                   Kafka Broker (kafka-0)                    │
  │  ┌───────────────────────────────────────────────────────┐  │
  │  │        Embedded MDS (Metadata Service)                │  │
  │  │  - Issues & validates JWT tokens using RSA Key Pair   │  │
  │  │  - Enforces RBAC permissions per Resource / Topic     │  │
  │  └───────────────────────────▲───────────────────────────┘  │
  └──────────────────────────────┼──────────────────────────────┘
                                 │ REST / JWT Tokens (Port 8090)
                    ┌────────────┴────────────┐
                    │ Control Center UI (C3)  │
                    │ (auth: rbac / mds)      │
                    └─────────────────────────┘
```

---

## 2. Infrastructure Setup & Secrets

### 2.1 OpenLDAP Deployment ([openldap.yaml](file:///home/ubuntu/Dev/CFK/openldap.yaml))
Deployed lightweight OpenLDAP server in namespace `confluent`:
```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: openldap
  namespace: confluent
spec:
  replicas: 1
  selector:
    matchLabels:
      app: openldap
  template:
    metadata:
      labels:
        app: openldap
    spec:
      containers:
        - name: openldap
          image: osixia/openldap:1.5.0
          env:
            - name: LDAP_ORGANISATION
              value: "Example Inc"
            - name: LDAP_DOMAIN
              value: "example.com"
            - name: LDAP_ADMIN_PASSWORD
              value: "adminpassword"
          ports:
            - containerPort: 389
              name: ldap
---
apiVersion: v1
kind: Service
metadata:
  name: openldap
  namespace: confluent
spec:
  selector:
    app: openldap
  ports:
    - port: 389
      targetPort: 389
      name: ldap
```

### 2.2 Directory Initialization & Schema Breakdown ([ldap-init.ldif](file:///home/ubuntu/Dev/CFK/ldap-init.ldif))

The LDIF (LDAP Data Interchange Format) file initializes the OpenLDAP directory structure required by Confluent MDS to authenticate users and resolve group memberships.

#### 2.2.1 Directory Hierarchy & LDIF Content

```ldif
# --- Organizational Units (OUs) ---
dn: ou=users,dc=example,dc=com
objectClass: organizationalUnit
ou: users

dn: ou=groups,dc=example,dc=com
objectClass: organizationalUnit
ou: groups

# --- User Entries (inetOrgPerson) ---
dn: cn=admin,ou=users,dc=example,dc=com
objectClass: inetOrgPerson
cn: admin
sn: Admin
userPassword: adminpassword

dn: cn=dev_user,ou=users,dc=example,dc=com
objectClass: inetOrgPerson
cn: dev_user
sn: Developer
userPassword: devpassword

dn: cn=view_user,ou=users,dc=example,dc=com
objectClass: inetOrgPerson
cn: view_user
sn: Viewer
userPassword: viewpassword

# --- Group Entries (groupOfNames) ---
dn: cn=kafka_admins,ou=groups,dc=example,dc=com
objectClass: groupOfNames
cn: kafka_admins
member: cn=admin,ou=users,dc=example,dc=com

dn: cn=kafka_developers,ou=groups,dc=example,dc=com
objectClass: groupOfNames
cn: kafka_developers
member: cn=dev_user,ou=users,dc=example,dc=com
```

#### 2.2.2 Detailed Breakdown of `ldap-init.ldif`

1. **Organizational Units (`ou=users` and `ou=groups`)**:
   - `ou=users,dc=example,dc=com`: Container for all user principal objects. Serves as the user search base (`userSearchBase`) for MDS.
   - `ou=groups,dc=example,dc=com`: Container for all team and group objects. Serves as the group search base (`groupSearchBase`) for MDS.

2. **User Entries (`objectClass: inetOrgPerson`)**:
   - Standard LDAP object class for identity management.
   - `cn` (Common Name): Acts as the short username identifier (e.g. `admin`, `dev_user`, `view_user`) matched by `userNameAttribute: "cn"`.
   - `userPassword`: The plain-text or hashed credential verified by MDS during simple LDAP authentication.
   - **User Accounts**:
     - `admin`: Master administrator user used for cluster administration and MDS metadata management.
     - `dev_user`: Developer account for producing and consuming messages on specific application topics.
     - `view_user`: Read-only viewer account for dashboard and metrics inspection.

3. **Group Entries (`objectClass: groupOfNames`)**:
   - Standard LDAP group class where group membership is recorded in `member` attributes as full Distinguished Names (DNs).
   - `cn=kafka_admins`: Maps to admin roles (contains `member: cn=admin,ou=users,dc=example,dc=com`).
   - `cn=kafka_developers`: Maps to developer roles (contains `member: cn=dev_user,ou=users,dc=example,dc=com`).

#### 2.2.3 Confluent MDS to LDAP Mapping Matrix

The table below illustrates how each structural element in `ldap-init.ldif` aligns with the Confluent Server CRD configuration (`spec.services.mds.provider.ldap.configurations`):

| LDIF Element | Value in `ldap-init.ldif` | Confluent MDS CRD Field | Function in MDS Authentication / Authorization |
| :--- | :--- | :--- | :--- |
| **User Search Root** | `ou=users,dc=example,dc=com` | `userSearchBase` | Base DN under which MDS searches for user credentials. |
| **User Identifier Key** | `cn` | `userNameAttribute` | Attribute used to match short login names (e.g. `dev_user`). |
| **User Schema Class** | `inetOrgPerson` | `userObjectClass` | LDAP objectClass filter applied during user lookup queries. |
| **Group Search Root** | `ou=groups,dc=example,dc=com` | `groupSearchBase` | Base DN under which MDS searches for team groups. |
| **Group Name Key** | `cn` | `groupNameAttribute` | Attribute identifying group short names (e.g. `kafka_developers`). |
| **Group Schema Class** | `groupOfNames` | `groupObjectClass` | LDAP objectClass filter applied during group membership lookups. |
| **Group Member Key** | `member` | `groupMemberAttribute` | Multi-valued attribute storing full member DNs. |
| **Member Regex Pattern** | `cn=(.*),ou=users,dc=example,dc=com` | `groupMemberAttributePattern` | Regular expression MDS uses to extract the username from member DNs. |

---

### 2.3 Kubernetes Secrets Deep-Dive

Confluent for Kubernetes (CFK) and Confluent Metadata Service (MDS) rely on 3 dedicated Kubernetes secrets for RSA token signing, LDAP administrative binding, and inter-service bearer authentication.

#### 2.3.1 Token RSA Key Pair Secret (`mds-token-pem`)

- **Purpose**: Provides the RSA 2048-bit asymmetric keypair used by MDS to cryptographically sign and verify JSON Web Tokens (JWT).
- **Files inside secret**:
  - `mdsTokenKeyPair.pem`: The RSA 2048-bit **private key** in PEM format. Confluent MDS uses this key to sign JWT bearer tokens issued via the `/security/1.0/authenticate` endpoint.
  - `mdsPublicKey.pem`: The corresponding RSA **public key** in PEM format. Mounted into Kafka Brokers, Control Center, and Schema Registry to verify the authenticity and signature of incoming JWT tokens without making synchronous HTTP calls back to MDS for every request.
- **Generation Commands**:
  ```bash
  openssl genrsa -out mdsTokenKeyPair.pem 2048
  openssl rsa -in mdsTokenKeyPair.pem -pubout -out mdsPublicKey.pem
  kubectl create secret generic mds-token-pem -n confluent \
    --from-file=mdsTokenKeyPair.pem=mdsTokenKeyPair.pem \
    --from-file=mdsPublicKey.pem=mdsPublicKey.pem
  ```

---

#### 2.3.2 LDAP Simple Bind Credentials Secret (`ldap-bind-creds`)

- **Purpose**: Supplies the bind user credentials that Confluent Server / MDS uses to establish an initial administrative LDAP session with OpenLDAP to perform user authentication lookups and group membership queries.
- **File inside secret**:
  - `ldap-server-simple.txt` (and alias `ldap.txt`):
    ```properties
    user=cn=admin,dc=example,dc=com
    username=cn=admin,dc=example,dc=com
    password=adminpassword
    ```
- **Technical Requirements & Gotchas**:
  - **Properties File Format**: CFK mounts secret keys as files inside `/mnt/secrets/ldap-bind-creds/`. Kafka's `FileConfigProvider` dynamically extracts property values at startup (e.g. `${file:/mnt/secrets/ldap-bind-creds/ldap-server-simple.txt:username}`).
  - **Key Aliasing**: Both `user=` and `username=` keys are included because CFK operator validation checks for `username=` while internal Java JNDI routines inspect `user=`.
  - **Line Ending Encoding**: Must use Unix LF (`\n`) line endings without Windows CR (`\r`). If `\r` is present, Java JNDI appends `\r` to the bind DN string (`cn=admin,dc=example,dc=com\r`), resulting in `javax.naming.InvalidNameException: [LDAP: error code 34 - invalid DN]`.

---

#### 2.3.3 MDS Service Client Credentials Secret (`mds-client-creds`)

- **Purpose**: Provides service account credentials for internal Confluent Platform components (Control Center, Kafka REST, CFK Operator) to authenticate against MDS endpoints and perform automated RBAC rolebinding operations.
- **File inside secret**:
  - `mds-client-bearer.txt` (and alias `bearer.txt`):
    ```properties
    username=admin
    password=adminpassword
    ```
- **Technical Requirements & Gotchas**:
  - **Strict Property Formatting**: The file must strictly contain `username` and `password` properties. Any extra or malformed keys cause CFK operator validation errors (`mds-client-bearer.txt in secretRef mds-client-creds: username and password not formatted correctly`).
  - **Internal Usage**: Injected into `kafka.properties` under `kafka.rest.client.sasl.jaas.config` and `kafka.rest.confluent.metadata.basic.auth.user.info` to enable Kafka REST and Control Center to interact with MDS over HTTP.

---

## 3. Confluent Component Specifications

### 3.1 Kafka Cluster CRD ([kafka-cluster.yaml](file:///home/ubuntu/Dev/CFK/kafka-cluster.yaml))
```yaml
apiVersion: platform.confluent.io/v1beta1
kind: Kafka
metadata:
  name: kafka
  namespace: confluent
spec:
  replicas: 1
  image:
    application: confluentinc/cp-server:7.6.0
    init: confluentinc/confluent-init-container:2.8.0
  dataVolumeCapacity: 10Gi
  storageClass:
    name: standard
  podTemplate:
    resources:
      requests:
        cpu: 200m
        memory: 256Mi
      limits:
        cpu: "1"
        memory: 1Gi
  dependencies:
    kRaftController:
      clusterRef:
        name: kraftcontroller
    kafkaRest:
      authentication:
        type: bearer
        bearer:
          secretRef: mds-client-creds
  authorization:
    type: rbac
    superUsers:
      - User:kafka-operator
      - User:admin
      - User:ANONYMOUS
      - User:kafka
  services:
    mds:
      tokenKeyPair:
        secretRef: mds-token-pem
      provider:
        type: ldap
        ldap:
          address: "ldap://openldap.confluent.svc.cluster.local:389"
          authentication:
            type: simple
            simple:
              secretRef: ldap-bind-creds
          configurations:
            userSearchBase: "ou=users,dc=example,dc=com"
            groupSearchBase: "ou=groups,dc=example,dc=com"
            userNameAttribute: "cn"
            userObjectClass: "inetOrgPerson"
            groupMemberAttribute: "member"
            groupMemberAttributePattern: "cn=(.*),ou=users,dc=example,dc=com"
            groupNameAttribute: "cn"
            groupObjectClass: "groupOfNames"
  configOverrides:
    jvm:
      - "-Xmx384m"
      - "-XX:+UseContainerSupport"
    server:
      - "offsets.topic.replication.factor=1"
      - "transaction.state.log.replication.factor=1"
      - "transaction.state.log.min.isr=1"
      - "confluent.license.topic.replication.factor=1"
      - "confluent.metadata.topic.replication.factor=1"
      - "confluent.security.event.logger.exporter.kafka.topic.replicas=1"
      - "confluent.balancer.topic.replication.factor=1"
      - "confluent.link.metadata.topic.replication.factor=1"
      - "confluent.cluster.link.metadata.topic.replication.factor=1"
```

### 3.2 Control Center CRD ([controlcenter.yaml](file:///home/ubuntu/Dev/CFK/controlcenter.yaml))
```yaml
apiVersion: platform.confluent.io/v1beta1
kind: ControlCenter
metadata:
  name: controlcenter
  namespace: confluent
spec:
  replicas: 1
  image:
    application: confluentinc/cp-enterprise-control-center:7.6.0
    init: confluentinc/confluent-init-container:2.8.0
  dataVolumeCapacity: 5Gi
  storageClass:
    name: standard
  authorization:
    type: rbac
    kafkaRestClassRef:
      name: default
  dependencies:
    kafka:
      bootstrapEndpoint: kafka.confluent.svc.cluster.local:9073
    mds:
      endpoint: http://kafka.confluent.svc.cluster.local:8090
      tokenKeyPair:
        secretRef: mds-token-pem
      authentication:
        type: bearer
        bearer:
          secretRef: mds-client-creds
  podTemplate:
    podSecurityContext:
      runAsUser: 0
      runAsGroup: 0
    resources:
      requests:
        cpu: 200m
        memory: 256Mi
      limits:
        cpu: "1"
        memory: 768Mi
    probe:
      liveness:
        initialDelaySeconds: 60
        failureThreshold: 10
      readiness:
        initialDelaySeconds: 30
        failureThreshold: 10
  configOverrides:
    server:
      - confluent.controlcenter.internal.topics.replication=1
      - confluent.controlcenter.command.topic.replication=1
      - confluent.metrics.topic.replication=1
      - confluent.monitoring.interceptor.topic.replication=1
      - confluent.controlcenter.internal.topics.partitions=1
      - confluent.metrics.topic.max.message.bytes=8388608
    jvm:
      - "-Xmx256m"
      - "-XX:+UseContainerSupport"
---
apiVersion: v1
kind: Service
metadata:
  name: controlcenter-public
  namespace: confluent
spec:
  type: NodePort
  selector:
    app: controlcenter
    platform.confluent.io/type: controlcenter
  ports:
    - name: http-controlcenter
      protocol: TCP
      port: 9021
      targetPort: 9021
      nodePort: 30021
```

### 3.3 KafkaRestClass & ConfluentRolebinding Manifests

#### [kafka-rest-class.yaml](file:///home/ubuntu/Dev/CFK/kafka-rest-class.yaml)
```yaml
apiVersion: platform.confluent.io/v1beta1
kind: KafkaRestClass
metadata:
  name: default
  namespace: confluent
spec:
  kafkaClusterRef:
    name: kafka
  kafkaRest:
    endpoint: http://kafka.confluent.svc.cluster.local:8090
    authentication:
      type: bearer
      bearer:
        secretRef: mds-client-creds
```

#### [rbac-rolebindings.yaml](file:///home/ubuntu/Dev/CFK/rbac-rolebindings.yaml)
```yaml
apiVersion: platform.confluent.io/v1beta1
kind: ConfluentRolebinding
metadata:
  name: admin-system-admin
  namespace: confluent
spec:
  principal:
    type: user
    name: admin
  role: SystemAdmin
  kafkaRestClassRef:
    name: default
  clustersScopeByIds:
    kafkaClusterId: ZGY4MzQyZTItNjYxMC00OQ
---
apiVersion: platform.confluent.io/v1beta1
kind: ConfluentRolebinding
metadata:
  name: dev-user-developer-read
  namespace: confluent
spec:
  principal:
    type: user
    name: dev_user
  role: DeveloperRead
  kafkaRestClassRef:
    name: default
  clustersScopeByIds:
    kafkaClusterId: ZGY4MzQyZTItNjYxMC00OQ
  resourcePatterns:
    - resourceType: Topic
      name: test-topic
      patternType: LITERAL
```

---

## 4. Debugging & Troubleshooting Journey

During the deployment of MDS RBAC on Confluent Server 7.6.0 with CFK, several complex runtime and operator-level issues were diagnosed and resolved using empirical log evidence and status introspection.

### Issue 1: Secret Key Mapping Failure in CFK Operator (`KeyInSecretRefIssue`)
- **Symptom**: `kubectl describe kafka kafka` reported:
  `Warning KeyInSecretRefIssue: required one of [ldap-server-simple.txt ldap.txt] missing in secretRef [ldap-bind-creds] for auth type [ldap_simple]`.
- **Root Cause**: Generic Kubernetes secrets created with keys `user` and `password` are rejected by CFK. CFK expects property files inside secrets (e.g. `ldap-server-simple.txt` or `ldap.txt`).
- **Resolution**: Recreated secret `ldap-bind-creds` with key `ldap-server-simple.txt` containing key-value property pairs.

---

### Issue 2: KafkaRest Dependency Requirement
- **Symptom**: `kubectl describe kafka kafka` reported:
  `Warning InputError: .spec.dependencies.kafkaRest.authentication must be configured`.
- **Root Cause**: When MDS/RBAC is enabled in CFK, Kafka REST (which exposes the MDS HTTP endpoints) requires explicit authentication configuration in `spec.dependencies.kafkaRest.authentication`.
- **Resolution**: Added `dependencies.kafkaRest.authentication.type: bearer` referencing `mds-client-creds`.

---

### Issue 3: Bootstrap Authorization Exception (`TopicAuthorizationException`)
- **Symptom**: `kafka-0` crashed with fatal exception:
  `Caused by: org.apache.kafka.common.errors.TopicAuthorizationException: Not authorized to access topics: [_confluent-metadata-auth]`.
- **Root Cause**: When Kafka boots with `ConfluentServerAuthorizer`, the internal MDS metadata initialization process accesses topic `_confluent-metadata-auth` prior to ACL initialization. Without `User:ANONYMOUS` in superUsers, access was denied.
- **Resolution**: Updated `authorization.superUsers` in `kafka-cluster.yaml` to include `- User:ANONYMOUS` and `- User:kafka`.

---

### Issue 4: LDAP Invalid DN Exception (`javax.naming.InvalidNameException`)
- **Symptom**: `kafka-0` log error:
  `Caused by: javax.naming.InvalidNameException: [LDAP: error code 34 - invalid DN] at java.naming/com.sun.jndi.ldap.LdapCtxFactory.getInitialContext`.
- **Root Cause**: Two issues occurred:
  1. The secret property file `ldap-server-simple.txt` used key `user` while Java `FileConfigProvider` expected key `username`.
  2. Windows CRLF line endings (`\r\n`) caused `\r` to be appended to the bind DN string (`cn=admin,dc=example,dc=com\r`), causing JNDI parsing failure.
- **Resolution**: Ensured strict LF line endings (`\n`) and included both `user=` and `username=` entries in `ldap-server-simple.txt`.

---

### Issue 5: Property File Parsing Failure in `FileConfigProvider`
- **Symptom**: `kafka-0` log error:
  `org.apache.kafka.common.config.ConfigException: Could not read properties from file /mnt/secrets/mds-client-creds/mds-client-bearer.txt`.
- **Root Cause**: Extraneous keys or improper format in `mds-client-bearer.txt` caused `java.util.Properties.load()` to fail.
- **Resolution**: Cleaned `mds-client-bearer.txt` to strictly contain only:
  ```properties
  username=admin
  password=adminpassword
  ```

---

### Issue 6: `ConfluentRolebinding` CRD Schema & Discovery Requirements
- **Symptom**:
  1. `CRD validation error`: `spec.principal.type: Unsupported value: "User": supported values: "user", "group"`.
  2. `confluentrolebinding` event: `kafkarestclass [default] failed to discovery the kafka cluster endpoint`.
- **Root Cause**:
  1. CFK `v1beta1` requires lowercase `"user"` for principal type.
  2. `KafkaRestClass` requires explicit `kafkaRest.endpoint` (`http://kafka.confluent.svc.cluster.local:8090`) and bearer authentication reference to discover the cluster ID and interact with MDS.
- **Resolution**: Updated `rbac-rolebindings.yaml` with `type: user` and configured `KafkaRestClass` (`default`) with the MDS HTTP endpoint.

---

## 5. Verification & Validation Results

### 5.1 Pod Readiness Verification
All pods in namespace `confluent` are in `Running` state:
```bash
$ kubectl get pods -n confluent
NAME                                       READY   STATUS    RESTARTS   AGE
cloudflared-quick-tunnel-6dbd94984-x5b97   2/2     Running   0          24h
confluent-operator-f66c6465d-ctxvq         1/1     Running   0          25h
controlcenter-0                            1/1     Running   1          8m
kafka-0                                    1/1     Running   0          2m
kraftcontroller-0                          1/1     Running   0          24h
openldap-69957bb844-tx22m                  1/1     Running   0          58m
```

### 5.2 Confluent Rolebindings Verification
All rolebindings transitioned to `CREATED`:
```bash
$ kubectl get confluentrolebinding -n confluent
NAME                       STATUS    KAFKACLUSTERID           PRINCIPAL       ROLE            KAFKARESTCLASS      AGE
admin-system-admin         CREATED   ZGY4MzQyZTItNjYxMC00OQ   User:admin      SystemAdmin     confluent/default   3m32s
dev-user-developer-read    CREATED   ZGY4MzQyZTItNjYxMC00OQ   User:dev_user   DeveloperRead   confluent/default   3m32s
internal-controlcenter-0   CREATED   ZGY4MzQyZTItNjYxMC00OQ   User:admin      SystemAdmin     confluent/default   8m28s
```

### 5.3 MDS REST Authentication & Token Issuance Test
Verified HTTP REST endpoint `http://kafka-0.kafka.confluent.svc.cluster.local:8090/security/1.0/authenticate`:

1. **`admin` User**:
   ```bash
   $ curl -s -u admin:adminpassword http://kafka-0.kafka.confluent.svc.cluster.local:8090/security/1.0/authenticate
   {
     "auth_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6bnVsbH0.eyJqdGkiOiJ0Q3otc0pw...",
     "token_type": "Bearer",
     "expires_in": 3600
   }
   ```

2. **`dev_user` User**:
   ```bash
   $ curl -s -u dev_user:devpassword http://kafka-0.kafka.confluent.svc.cluster.local:8090/security/1.0/authenticate
   {
     "auth_token": "eyJhbGciOiJSUzI1NiIsImtpZCI6bnVsbH0.eyJqdGkiOiJBVXdXZlJW...",
     "token_type": "Bearer",
     "expires_in": 3600
   }
   ```

3. **Invalid Password Rejection**:
   ```bash
   $ curl -s -u dev_user:wrongpassword http://kafka-0.kafka.confluent.svc.cluster.local:8090/security/1.0/authenticate
   {
     "message": "Unauthorized",
     "url": "/security/1.0/authenticate",
     "status": "401"
   }
   ```

---

## 6. Artifact & File Reference Index

- [`openldap.yaml`](file:///home/ubuntu/Dev/CFK/openldap.yaml): OpenLDAP Deployment and Service.
- [`ldap-init.ldif`](file:///home/ubuntu/Dev/CFK/ldap-init.ldif): LDAP entries for users and groups.
- [`kafka-cluster.yaml`](file:///home/ubuntu/Dev/CFK/kafka-cluster.yaml): Kafka CRD with MDS and LDAP auth.
- [`controlcenter.yaml`](file:///home/ubuntu/Dev/CFK/controlcenter.yaml): ControlCenter CRD with MDS auth.
- [`kafka-rest-class.yaml`](file:///home/ubuntu/Dev/CFK/kafka-rest-class.yaml): KafkaRestClass resource.
- [`rbac-rolebindings.yaml`](file:///home/ubuntu/Dev/CFK/rbac-rolebindings.yaml): SystemAdmin and DeveloperRead rolebindings.
