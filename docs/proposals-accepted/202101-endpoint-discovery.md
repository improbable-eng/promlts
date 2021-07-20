---
type: proposal
title: Unified Endpoint Discovery
status: accepted
owner: lilic
menu: proposals-accepted
---

### Related Tickets

* https://github.com/thanos-io/thanos/issues/2600
* https://github.com/thanos-io/thanos/issues/3538

### Summary

We want to propose a new flag called `--endpoint=<address>` that will be passed to Thanos query and replace the current `--store=<address>` and `--rule=<address>` flags, and add a discovery mechanism which will be used to determine what types of services this endpoint can serve.

### Motivation

Currently, in Thanos Query, the discovery of rules APIs happens via Store API's Info method. This makes it harder if we ever want to not have a coupling to the Store API (which is already planned for [scalable ruler proposal](https://github.com/thanos-io/thanos/blob/main/docs/proposals-accepted/202005-scalable-rule-storage.md)).

We also require passing two different flags to the Thanos Query component `--store=<address>` and `--rule=<address>`. If users use both flags, the Query component performs DNS discovery on often the same address multiple times, which can cause occasional DNS issues as this resolution happens so frequently. This is especially confusing when one DNS lookup works and the other doesn't. Adding new APIs in the future would exacerbate this issue with DNS requests.

### Goals

Unite discovery of endpoints, which would make code cleaner as well as provide building blocks to provide discovery and support for new gRPC services, for example, the Targets API/Exemplar API/Metadata API.

### Solution

Add a new flag called `--endpoint` to Thanos query, and auto-discover what services that endpoint is serving based on metadata each gRPC server exposes.

Each component will expose an Info service, that includes various metadata listed below. Discovery of what services an endpoint is exposing will happen via [gRPC reflection](https://github.com/grpc/grpc/blob/master/doc/server-reflection.md). Once we discover what services that endpoint is exposing (e.g. StoreAPI, TargetsAPI) we can retrieve metadata about them using a single `Info` method call from the Info service.

Info API metadata would include the following fields regardless of the type:

```
info:
  external_labels: blah_1, blah_2, ...
  store:
    minTime: 2
    maxTime: 4
  rule:
    blah: ...
```

Right now this Info service would be added to existing components exposing a gRPC server.

The upgrade path would be possible in this case, as it's strictly additive for endpoints, and there will be a migration path to fall back to current discovery methods, which could be removed in the future.

### Alternatives

Initial discovery of gRPC services available will happen via gRPC reflection. To get further information, we would be adding an Info method to other services for example, Targets, Exemplars, etc. This would contain similar info or metadata information as we get right now from the Store Info method. We can always add more data to it in the future as needed.

This alternative solution was also considered, however it was not chosen because of its following drawbacks.

1. For servers implementing multiple APIs (e.g. Store, Rules, Exemplar), we need to make multiple gRPC calls, e.g. three calls to a component implementing Store, Rules and Exemplars APIs.
2. There is also ambiguity about which Info method should be used for these kinds of servers.

### Upgrade plan

Regardless of the solution, we will be not removing any flags for a given period and have a fallback in place to be able to discover in the same way as we do right now. This will ensure a smooth upgrade path for the users who make use of the current flags for discovery. We will mark the current flags as deprecated and after two releases have passed we can remove the existing component-specific discovery flags from the query component and remove any migration code.

### Work plan

1. We would be adding the new Info service and flags needed for the above solution, so this part would be entirely additive.
2. Second step would be the integration of this service, the addition of new flags, and migration of existing code.
3. We would not be removing the existing flags (`--store` and `--rule`) for some grace period, after that passes we would also have to remove it and any migration code.
