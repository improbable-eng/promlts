# Configuring Thanos Secure TLS Cross-Cluster Communication

###### *This guide was contributed by the community thanks to [gmintoco](https://github.com/gmintoco)*

With some scale in global view Thanos mode, without [Thanos Receive](../components/receive.md), you often have centralized clusters that require secure, TLS gRPC routes to remote clusters outside your network to access leaf Prometheus-es with sidecars. Common solutions like VPC peering and VPN might be complex to setup, expensive and not easy to manage. In this guide we will explain setting up server proxies to establish secure route for queries.

## Scenario

Let's imagine we have an `Observer Cluster` that is hosting [Thanos Querier](../components/query.md) along with [Thanos Store Gateway](../components/store.md). In same cluster we also have one or more [Thanos Sidecars](../components/sidecar.md) that you would like to connect to within the cluster.

However let's say we also need to connect from the observer cluster's querier to several remote instances of Thanos Sidecar in remote clusters. (For example their NGINX Ingress, of which the configs below are based on).

Ideally, we want to use TLS to encrypt the connection to the remote clusters, but we don't want to use TLS within the cluster (to reduce no. of ingresses, pain of provisioning certificates etc.) We may also want to use client certificate authentication to these remote clusters for improved security (see envoy v3 example below).

In this scenario you need to use a proxy server. Further guidance below.

## Proxy based communication using Envoy

Envoy can be implemented as a sidecar container (example shown here) within the Thanos Querier pod on the Observer Cluster. It will perform TLS origination to connect to secure remote sidecars while forwarding their communications unencrypted back, locally to Thanos Querier.

[Envoy](https://www.envoyproxy.io/) is a proxy server that has good HTTP2 and gRPC support and is relatively straightforward to configure for this purpose.

- Configure an envoy sidecar container to the Thanos Querier pod (unfortunately this also isn't supported by a lot of Thanos charts) an example pod config is below (see `deployment.yaml`)
- Make sure that the envoy sidecar has the correct certificates (using a mounted secret) and a valid configuration (using a mounted configmap) an example envoy config is below (`envoy.yaml`)
- Configure a service for the envoy sidecar an example service is shown below (`service.yaml`) you may have another service already for local cluster access (for Thanos Ruler or Grafana etc.)
- Point the querier at the service and the correct port an example `--store ` field is below (`thanos-querier args`)
- Make sure your remote cluster has TLS setup and an appropriate HTTP2 supported ingress, example below `ingress.yaml`

### Observer Cluster: Querier with Envoy `deployment.yaml`

- `[port_name]` is the name of the port specified within the service (see `service.yaml`)
- `[service-name]` is the name of the envoy service
- `[namespace]` is the name of the envoy service namespace

You may need to change cluster.local depending on your cluster domain. The `--store` entries for thanos storegateway etc. may be named different in your setup

```yaml
kind: Deployment
apiVersion: apps/v1
metadata:
  name: thanos-global-test-querier
  namespace: thanos-global
  labels:
    name: thanos-global-test-querier
  replicas: 2
  selector:
    matchLabels:
      name: thanos-global-test-querier
  template:
    metadata:
      labels:
        name: thanos-global-test-querier
    spec:
      volumes:
        - name: config
          configMap:
            name: thanos-global-test-envoy-config
            defaultMode: 420
            optional: false
        - name: certs
          secret:
            secretName: thanos-global-test-envoy-certs
            defaultMode: 420
            optional: false
      containers:
        - name: querier
          image: 'thanosio/thanos:v0.17.2'
          args:
            - query
            - '--log.level=info'
            - '--grpc-address=0.0.0.0:10901'
            - '--http-address=0.0.0.0:10902'
            - '--query.replica-label=replica'
            - >-
              --store=dnssrv+_grpc._tcp.thanos-global-test-storegateway.thanos-global.svc.cluster.local
            - >-
              --store=dnssrv+_grpc._tcp.thanos-global-test-sidecar.thanos-global.svc.cluster.local
            - >-
              --store=dnssrv+_grpc._tcp.thanos-global-test-ruler.thanos-global.svc.cluster.local
            - >-
              --store=dnssrv+_[port_name]._tcp.[service-name].[namespace].svc.cluster.local
            - >-
              --store=dnssrv+_[port_name_2]._tcp.[service-name].[namespace].svc.cluster.local
          ports:
            - name: http
              containerPort: 10902
              protocol: TCP
            - name: grpc
              containerPort: 10901
              protocol: TCP
          resources: {}
          livenessProbe:
            httpGet:
              path: /-/healthy
              port: http
              scheme: HTTP
            initialDelaySeconds: 30
            timeoutSeconds: 1
            periodSeconds: 30
            successThreshold: 1
            failureThreshold: 3
          readinessProbe:
            httpGet:
              path: /-/ready
              port: http
              scheme: HTTP
            initialDelaySeconds: 30
            timeoutSeconds: 1
            periodSeconds: 30
            successThreshold: 1
            failureThreshold: 3
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
          imagePullPolicy: IfNotPresent
          securityContext:
            privileged: false
            runAsUser: 1001
            runAsGroup: 0
            runAsNonRoot: false
            readOnlyRootFilesystem: false
            allowPrivilegeEscalation: true
        - name: envoy-sidecar
          image: 'envoyproxy/envoy:v1.16.0'
          args:
            - '-c'
            - /config/envoy.yaml
            - '-l'
            - debug
          ports:
            - name: [port_name]
              containerPort: 10000
              protocol: TCP
            - name: [port_name_2]
              containerPort: 10001
              protocol: TCP
          resources: {}
          volumeMounts:
            - name: config
              mountPath: /config
              mountPropagation: None
            - name: certs
              mountPath: /certs
              mountPropagation: None
          terminationMessagePath: /dev/termination-log
          terminationMessagePolicy: File
          imagePullPolicy: IfNotPresent
      restartPolicy: Always
      terminationGracePeriodSeconds: 30
      dnsPolicy: ClusterFirst
      serviceAccountName: thanos-global-test-querier-sa
      serviceAccount: thanos-global-test-querier-sa
      automountServiceAccountToken: false
      shareProcessNamespace: false
      securityContext: {}
      schedulerName: default-scheduler
  strategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 25%
      maxSurge: 25%
  revisionHistoryLimit: 10
  progressDeadlineSeconds: 600
```

### Forward proxy Envoy configuration `envoy.yaml`

This is a static v2 envoy configuration (v3 example below). You will need to update this configuration for every sidecar you would like to talk to. There are also several options for dynamic configuration, like envoy XDS (and other associated dynamic config modes), or using something like terraform (if thats your deployment method) to generate the configs at deployment time. NOTE: This config **does not** send a client certificate to authenticate with remote clusters, see envoy v3 config.

```yaml
admin:
  access_log_path: /tmp/admin_access.log
  address:
    socket_address: { address: 0.0.0.0, port_value: 9901 }
​
static_resources:
  listeners:
  - name: sidecar_name
    address:
      socket_address: { address: 0.0.0.0, port_value: 10000 }
    filter_chains:
    - filters:
      - name: envoy.http_connection_manager
        config:
          codec_type: auto
          stat_prefix: ingress_http
          route_config:
            name: local_route
            virtual_hosts:
            - name: local_service
              domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route: { cluster: sidecar_name, host_rewrite: thanos.sidecardomain.com }
          http_filters:
          - name: envoy.router
  - name: sidecar_name_2
    address:
      socket_address: { address: 0.0.0.0, port_value: 10001 }
    filter_chains:
    - filters:
      - name: envoy.http_connection_manager
        config:
          codec_type: auto
          stat_prefix: ingress_http
          route_config:
            name: local_route
            virtual_hosts:
            - name: local_service
              domains: ["*"]
              routes:
              - match: { prefix: "/" }
                route: { cluster: sidecar_name_2, host_rewrite: thanos.sidecardomain.com }
          http_filters:
          - name: envoy.router
  clusters:
  - name: sidecar_name
    connect_timeout: 30s
    type: logical_dns
    http2_protocol_options: {}
    dns_lookup_family: V4_ONLY
    lb_policy: round_robin
    hosts: [{ socket_address: { address: thanos.sidecardomain.com, port_value: 443 }}]
    tls_context:
      common_tls_context:
        validation_context:
          trusted_ca:
            filename: /certs/ca.crt
        alpn_protocols:
        - h2
        - http/1.1
      sni: thanos.sidecardomain.com
  - name: sidecar_name_2
    connect_timeout: 30s
    type: logical_dns
    http2_protocol_options: {}
    dns_lookup_family: V4_ONLY
    lb_policy: round_robin
    hosts: [{ socket_address: { address: thanos-2.sidecardomain.com, port_value: 443 }}]
    tls_context:
      common_tls_context:
        validation_context:
          trusted_ca:
            filename: /certs/ca.crt
        alpn_protocols:
        - h2
        - http/1.1
      sni: thanos-2.sidecardomain.com
```

### `envoy.yaml` V3 API

This is an example envoy config using the v3 API. It does differ slightly to the above (more log formatting) but is essentially the same in functionality. This config **sends** a client certificate to authenticate with remote clusters (they must have the CA loaded in order to verify). This only implements a single port/listener, but adding more (ie. the v2 example has 2) is fairly trivial. Simple clone the `sidecar_name` listener and the `sidecar_name` cluster blocks.

```yaml
admin:
  access_log_path: /tmp/admin_access.log
  address:
    socket_address: { address: 0.0.0.0, port_value: 9901 }

static_resources:
  listeners:
  - name: sidecar_name
    address:
      socket_address:
        address: 0.0.0.0
        port_value: 10001
    filter_chains:
    - filters:
      - name: envoy.http_connection_manager
        typed_config:
          "@type": type.googleapis.com/envoy.extensions.filters.network.http_connection_manager.v3.HttpConnectionManager
          codec_type: AUTO
          access_log:
          - name: envoy.access_loggers.file
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog
              path: /dev/stdout
              log_format:
                text_format: |
                  [%START_TIME%] "%REQ(:METHOD)% %REQ(X-ENVOY-ORIGINAL-PATH?:PATH)% %PROTOCOL%"
                  %RESPONSE_CODE% %RESPONSE_FLAGS% %RESPONSE_CODE_DETAILS% %BYTES_RECEIVED% %BYTES_SENT% %DURATION%
                  %RESP(X-ENVOY-UPSTREAM-SERVICE-TIME)% "%REQ(X-FORWARDED-FOR)%" "%REQ(USER-AGENT)%"
                  "%REQ(X-REQUEST-ID)%" "%REQ(:AUTHORITY)%" "%UPSTREAM_HOST%" "%UPSTREAM_TRANSPORT_FAILURE_REASON%"\n
          - name: envoy.access_loggers.file
            typed_config:
              "@type": type.googleapis.com/envoy.extensions.access_loggers.file.v3.FileAccessLog
              path: /dev/stdout
          stat_prefix: ingress_http
          route_config:
            name: local_route
            virtual_hosts:
            - name: local_service
              domains: ["*"]
              routes:
              - match:
                  prefix: "/"
                route:
                  cluster: sidecar_name
                  host_rewrite_literal: thanos.sidecardomain.com
          http_filters:
          - name: envoy.filters.http.router
  clusters:
  - name: sidecar_name
    connect_timeout: 30s
    type: LOGICAL_DNS
    http2_protocol_options: {}
    dns_lookup_family: V4_ONLY
    lb_policy: ROUND_ROBIN
    load_assignment:
      cluster_name: sidecar_name
      endpoints:
        - lb_endpoints:
          - endpoint:
              address:
                socket_address:
                  address: thanos.sidecardomain.com
                  port_value: 443
    transport_socket:
      name: envoy.transport_sockets.tls
      typed_config:
        "@type": type.googleapis.com/envoy.extensions.transport_sockets.tls.v3.UpstreamTlsContext
        common_tls_context:
          tls_certificates:
            - certificate_chain:
                filename: /certs/tls.crt
              private_key:
                filename: /certs/tls.key
          validation_context:
            trusted_ca:
              filename: /certs/cacerts.pem
          alpn_protocols:
          - h2
          - http/1.1
        sni: thanos.sidecardomain.com

```

### Observer Cluster: Querier with Envoy `service.yaml`

This is the service for the envoy sidecar. You will need to define a new port for every sidecar you would like to add.

```yaml
kind: Service
apiVersion: v1
metadata:
  name: thanos-global-test-envoy
  namespace: thanos-global
  labels:
    name: thanos-global-test-envoy
spec:
  ports:
    - name: [port_name]
      protocol: TCP
      port: 10000
      targetPort: 10000
    - name: [port_name_2]
      protocol: TCP
      port: 10001
      targetPort: 10001
  selector:
    name: thanos-global-test-querier
  type: ClusterIP
  sessionAffinity: None
```

### Client clusters: Sidecarc`ingress.yaml`

This is an example ingress for a remote sidecar using NGINX ingress. You must use TLS (port 443 - limitation from NGINX) as HTTP2 is only supported on a separate listener (see [here](https://github.com/kubernetes/ingress-nginx/issues/3938))

You must have certs configured and the CA added into the envoy sidecar earlier to allow verification (if using client cert v3 envoy config)

```yaml
kind: Ingress
apiVersion: extensions/v1beta1
metadata:
  name: monitoring-rancher-monitor-thanos-gateway
  namespace: monitoring
  annotations:
    nginx.ingress.kubernetes.io/backend-protocol: GRPC
    nginx.ingress.kubernetes.io/force-ssl-redirect: 'true'
    nginx.ingress.kubernetes.io/grpc-backend: 'true'
    nginx.ingress.kubernetes.io/protocol: h2c
    nginx.ingress.kubernetes.io/proxy-read-timeout: '160'
spec:
  tls:
    - hosts:
        - thanos.sidecardomain.com
      secretName: thanos-tls
  rules:
    - host: thanos.sidecardomain.com
      http:
        paths:
          - path: /
            backend:
              serviceName: monitoring-rancher-monitor-prometheus
              servicePort: 10901
```

## Summary

This has outlined a scenario, potential solution and a collection of example configurations. After implementing a setup like this you can expect to be able to have a central Thanos instance that can access sidecars, store gateway's, receivers through standard unsecured gRPC etc. while simultaneously accessing resources (e.g StoreAPIs of sidecars etc.) located externally in a secure fashion using client-cert authentication and HTTPS/TLS encryption.
