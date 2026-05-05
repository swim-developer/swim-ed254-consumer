# SWIM ED-254 Consumer, Raw YAML Deployment

## Prerequisites

- `oc` CLI authenticated to an OpenShift cluster
- Namespace `swim-demo` exists
- MongoDB deployed with `mongodb-credentials` secret in `swim-demo`
- Kafka cluster available
- AMQP broker (Artemis) available

## Deploy Order

Apply the manifests in this exact order. Each step depends on the previous one.

```bash
# 1. ConfigMap (application configuration, must exist before the Deployment)
oc apply -f swim-ed254-consumer-config.yaml -n swim-demo

# 2. mTLS Secret (certificates, must exist before the Deployment)
oc apply -f swim-ed254-consumer-mtls-secret.yaml -n swim-demo

# 3. Service (must exist before ServiceMonitor)
oc apply -f swim-ed254-consumer-service.yaml -n swim-demo

# 4. Deployment (depends on ConfigMap, mTLS Secret, and mongodb-credentials)
oc apply -f swim-ed254-consumer-deployment.yaml -n swim-demo

# 5. HorizontalPodAutoscaler (references the Deployment)
oc apply -f swim-ed254-consumer-hpa.yaml -n swim-demo

# 6. ServiceMonitor (references the Service, requires Prometheus Operator)
oc apply -f swim-ed254-consumer-servicemonitor.yaml -n swim-demo
```

## Teardown

Remove in reverse order:

```bash
oc delete -f swim-ed254-consumer-servicemonitor.yaml -n swim-demo
oc delete -f swim-ed254-consumer-hpa.yaml -n swim-demo
oc delete -f swim-ed254-consumer-deployment.yaml -n swim-demo
oc delete -f swim-ed254-consumer-service.yaml -n swim-demo
oc delete -f swim-ed254-consumer-mtls-secret.yaml -n swim-demo
oc delete -f swim-ed254-consumer-config.yaml -n swim-demo
```
