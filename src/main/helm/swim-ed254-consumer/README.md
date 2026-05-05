# SWIM ED-254 Consumer -- Helm Chart

## Prerequisites

- Helm 3.x installed
- `kubectl` or `oc` CLI authenticated to your cluster
- Namespace `swim-demo` exists
- MongoDB deployed with `mongodb-credentials` secret
- Kafka and AMQP broker (Artemis) available

## Quick Start

### OpenShift / OpenShift Local (CRC)

```bash
helm install swim-ed254-consumer . -n swim-demo
```

### Kubernetes / minikube

```bash
helm install swim-ed254-consumer . -n swim-demo

kubectl port-forward svc/swim-ed254-consumer 8080:8080 -n swim-demo
```

## Customizing Values

```bash
# Change image tag
helm install swim-ed254-consumer . -n swim-demo \
  --set image.tag=1.2.0

# Change replicas and resources
helm install swim-ed254-consumer . -n swim-demo \
  --set replicas=3 \
  --set resources.requests.memory=512Mi

# Disable HPA or ServiceMonitor
helm install swim-ed254-consumer . -n swim-demo \
  --set hpa.enabled=false \
  --set serviceMonitor.enabled=false
```

### Key Values

| Parameter | Default | Description |
|-----------|---------|-------------|
| `namespace` | `swim-demo` | Target namespace |
| `image.repository` | `quay.io/masales/swim-ed254-consumer` | Container image |
| `image.tag` | `latest` | Image tag |
| `replicas` | `1` | Number of replicas |
| `resources.requests.memory` | `256Mi` | Memory request |
| `resources.limits.memory` | `512Mi` | Memory limit |
| `hpa.enabled` | `true` | Enable autoscaling |
| `hpa.maxReplicas` | `5` | Maximum replicas |
| `serviceMonitor.enabled` | `true` | Enable Prometheus metrics |

## Upgrade

```bash
helm upgrade swim-ed254-consumer . -n swim-demo
```

## Uninstall

```bash
helm uninstall swim-ed254-consumer -n swim-demo
```

## Platform Compatibility

| Resource | OpenShift | OpenShift Local | Kubernetes | minikube |
|----------|-----------|-----------------|------------|----------|
| Deployment | Yes | Yes | Yes | Yes |
| Service | Yes | Yes | Yes | Yes |
| ConfigMap | Yes | Yes | Yes | Yes |
| Secret | Yes | Yes | Yes | Yes |
| HPA | Yes | Yes | Yes | Yes |
| ServiceMonitor | Yes (1) | Yes (1) | Yes (1) | Yes (1) |

(1) Requires Prometheus Operator installed in the cluster.
