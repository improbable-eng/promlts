---
type: proposal
title: Expose Thanos APIs to OpenAPI/protobuf and expose on website and UIs
status: accepted
owner: Hangzhi
menu: proposals-accepted
Date: July 2021
---

## **Expose Thanos APIs to OpenAPI/protobuf and expose on website and UIs**

- Owners:
  - [@Hangzhi](https://github.com/Hangzhi)
- Related Tickets:
  - Program Proposal: https://github.com/thanos-io/thanos/issues/4102
  - Need for auto-documentation in Prometheus:
    - https://github.com/prometheus/prometheus/issues/7192
    - https://github.com/prometheus/prometheus/issues/5567
- Other docs:
  - A proposal of LFX mentorship: https://docs.google.com/document/d/1GeyFCk4QkVSrx4pQ4OSmOA1KE_FvmJRi_aq9hr2GoZQ/edit?usp=sharing

This design doc is proposing a design for Thanos HTTP API defined in protobuf and OpenAPI.

## **Motivations**

To improve Thanos usage for users, we would like to define our HTTP APIs in protobuf/OpenAPI and expose those in the repository.
OpenAPI is a language for describing REST APIs with a widely-used tooling ecosystem. With OpenAPI, developers can generate live documentation, validate APIs and even generate client and server stubs from OpenAPI to use our APIs efficiently. 
Also, the auto-generated documentation problem prevent documentation errors  ([Prometheus#7192](https://github.com/prometheus/prometheus/issues/7192), [Prometheus#5567](https://github.com/prometheus/prometheus/issues/5567)).
Protocol Buffers (a.k.a., protobuf) is well-known as a mechanism for serializing structured data, and it's usually used to define gRPC APIs. Also, protobuf specification could be used to define REST API. 
We hope to use protobuf to define our APIs for consistency, while we also want to leverage the tooling ecosystem of OpenAPI. 

So, we want to define REST APIs in protobuf, generate OpenAPI definition from protobuf with [gnostic](https://github.com/google/gnostic). This would allow users to use tools for documentation, validation, type checking, and even interface code generation to use our APIs efficiently.

Similarly, we want to reuse this work in Prometheus.


### **Pitfalls of the current solution**

- Documentation, Server code, and client code boilerplate needs to be written manually.
- Hand-written interface code is expensive to write and hard to maintain. 
- It's hard to discover the current API programmatically.
- When modifying the API, it is very difficult to know if this breaks downstream users. 

## **Goals**

- Define all APIs in protobuf.
- Be able to generate OpenAPI3 from protobuf.
- Auto-generate documentation with OpenAPI3 specification.
- Generate server code from API specification (OpenAPI3 or protobuf).
- Define all configuration potentially in protobuf too: https://github.com/openproto/protoconfig.

## **Non-Goals**

- Don't mix gRPC with HTTP APIs in the same protobuf package

## **How**

- Define Thanos APIs in protobuf specification.
- Generate OpenAPI from protobuf with gnostic extension.
- Generate documentation from OpenAPI with [swagger](https://github.com/swagger-api/swagger-codegen).
- Generate server and client API stubs potentially from OpenAPI with [swagger](https://github.com/swagger-api/swagger-codegen) or [oapi-codegen](https://github.com/deepmap/oapi-codegen).
## **Alternatives**

## **Define HTTP RESTful API only in OpenAPI. Not in protobuf.**

1. Pros:
   1. There are might be some complexity, edge case and extra tooling to make the 3-step process (proto -> OpenAPI -> documentation to work.
    
2. Cons:
   1. We write API definitions in protobuf for consistency. OpenAPI is less consistent compared to protobuf in a project built on Golang. 
   2. Protobuf definition is more concise to write. 

## **Define APIs in gRPC and have RESTful APIs alongside with [grpc - gateway](https://github.com/grpc-ecosystem/grpc-gateway)**

1. Pros:
   1. We can have gRPC and RESTful APIs at the same time.
   2. We have gRPC APIs like rules API.
2. Cons:
   1. There are might be some complexity, edge cases and extra tooling to make the process (define gRPC API and get RESTful APIs with grpc-gateway) work. 
   2. We need to redefine our API service in gPRC since most of Thanos APIs and Prometheus APIs are RESTful APIs. 
   3. We need to run another sidecar (complexity of running the system).
   4. Semantics of gRPC and HTTP might be different and surprising for end user. 
   5. We want to reuse in Prometheus and Prometheus does not support gRPC (gRPC dependency was removed from codebase).
   6. Same port library is not maintained. (cmux)

## **Action Plan**

- [ ]  Experiments in rules API.
- [ ]  Define all http APIs in Protobuf.
- [ ]  Generate documents, client code, and server stubs from OpenAPI specification.

