# Dual Stack Deployment Guide

## Project Structure

```
RabbitMQ-K8S/
├── producer.py                      # Python FastAPI producer
├── consumer.py                      # Python consumer
├── Dockerfile                       # Python image
├── requirements.txt                 # Python dependencies
├── secrets.yaml                     # Python stack secrets
├── rabbitmq-service.yaml           # Shared RabbitMQ (default namespace)
├── postgres-service.yaml           # PostgreSQL for Python
├── incident-apps.yaml              # Python producer + consumers
└── dotnet-stack/                   # .NET Stack (NEW)
    ├── SoapProducer/
    │   ├── Program.cs
    │   ├── Models.cs
    │   ├── IncidentService.cs
    │   ├── SoapProducer.csproj
    │   └── Dockerfile
    ├── DotnetConsumer/
    │   ├── Program.cs
    │   ├── MessageHandlers.cs
    │   ├── SqlServerRepository.cs
    │   ├── DotnetConsumer.csproj
    │   └── Dockerfile
    ├── k8s-manifests/
    │   ├── namespaces.yaml
    │   ├── dotnet-secrets.yaml
    │   ├── sqlserver-deployment.yaml
    │   ├── soap-producer-deployment.yaml
    │   └── dotnet-consumer-deployment.yaml
    ├── sql-scripts/
    │   └── init.sql
    └── DEPLOYMENT.md               # This file
```

---

## Prerequisites

- Minikube installed and running
- Docker CLI
- kubectl installed

---

## Quick Start

### 1. Point Docker at Minikube
```powershell
& minikube -p minikube docker-env --shell powershell | Invoke-Expression
```

### 2. Create Namespaces
```bash
cd RabbitMQ-K8S
kubectl apply -f dotnet-stack/k8s-manifests/namespaces.yaml
```

### 3. Deploy RabbitMQ
```bash
kubectl apply -f secrets.yaml
kubectl apply -f rabbitmq-service.yaml
kubectl get pods -w  # Wait for Running
```

### 4. Build Images
```bash
# Python
docker build -t incident-app:v4 .

# SOAP API
cd dotnet-stack/SoapProducer
docker build -t soap-producer:v1 .
cd ../..

# .NET Consumer
cd dotnet-stack/DotnetConsumer
docker build -t dotnet-consumer:v1 .
cd ../..
```

### 5. Deploy Python Stack
```bash
kubectl apply -f secrets.yaml
kubectl apply -f postgres-service.yaml
kubectl apply -f incident-apps.yaml
```

### 6. Deploy .NET Stack
```bash
kubectl apply -f dotnet-stack/k8s-manifests/dotnet-secrets.yaml
kubectl apply -f dotnet-stack/k8s-manifests/sqlserver-deployment.yaml
kubectl apply -f dotnet-stack/k8s-manifests/soap-producer-deployment.yaml
kubectl apply -f dotnet-stack/k8s-manifests/dotnet-consumer-deployment.yaml
```

### 7. Port Forward (3 terminals)
```bash
# Terminal 1
kubectl port-forward service/producer-api-svc 8080:80

# Terminal 2
kubectl port-forward service/soap-producer-service 8081:80 -n dotnet-stack

# Terminal 3
kubectl port-forward service/ha-rabbitmq 15672:15672
```

---

## Access URLs

- **FastAPI**: http://localhost:8080/docs
- **SOAP API**: http://localhost:8081/IncidentService.asmx?wsdl
- **RabbitMQ**: http://localhost:15672
---

## Queue Isolation

**Python**: `py_*` prefix  
**.NET**: `dotnet_*` prefix

View in RabbitMQ UI → Queues tab

---

## Troubleshooting

**ImagePullBackOff:**
```powershell
& minikube docker-env --shell powershell | Invoke-Expression
docker images  # Verify images exist
```

**SQL Server not starting:**
```bash
kubectl logs <sqlserver-pod> -n dotnet-stack
# Needs ~2GB RAM
```

**Check logs:**
```bash
kubectl logs <pod-name>
kubectl logs <pod-name> -n dotnet-stack
```

---

## Clean Up

```bash
kubectl delete namespace dotnet-stack
kubectl delete -f incident-apps.yaml
kubectl delete -f postgres-service.yaml
kubectl delete -f rabbitmq-service.yaml
```

---

Full documentation in the complete DEPLOYMENT.md file above.
