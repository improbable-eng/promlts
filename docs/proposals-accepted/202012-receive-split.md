---
type: proposal
title: Thanos Routing Receive and Ingesting Receive
status: accepted
menu: proposals-accepted
---

### Related Tickets

* Hashring improvements: https://github.com/thanos-io/thanos/issues/3141
* Previous proposal implementations: https://github.com/thanos-io/thanos/pull/3845, https://github.com/thanos-io/thanos/pull/3580
* Distribution idea: https://github.com/thanos-io/thanos/pull/2675

### Summary

This document describes the motivation and design of running **Receiver** in a stateless mode that does not have capabilities to store samples, it only routes remote write to further receivers based on hashring.

This allows setting optional deployment model were only ***routing receivers*** are using hashring files and does the routing and replication. That allows ***ingesting receivers*** to not handle any routing or hashring, only receiving multi tenant writes.

### Motivation

[@squat](https://github.com/squat):

> Currently, any change to the hashring configuration file will trigger all Thanos Receive nodes to flush their multi-TSDBs, causing them to enter an unready state until the flush is complete. This unavailability during a flush allows for a clear state transition, however it can result in downtimes on the order of five minutes for every configuration change. Moreover, during configuration changes, the hashring goes through an even longer period of partial unreadiness, where some nodes begin and finish flushing before and after others. During this partial unreadiness, the hashring can expect high internal request failure rates, which cause clients to retry their requests, resulting in even higher load. Therefore, when the hashring configuration is changed due to automatic horizontal scaling of a set of Thanos Receivers, the system can expect higher than normal resource utilization, which can create a positive feedback loop that continuously scales the hashring.

### Goals

* Reduce downtime of the ingestion logic in Thanos Receiver.

### Proposal

We propose allowing to run Thanos Receiver in a mode that only forwards/replicates remote write (distributor mode) apart from the ingesting mode (default mode). You can enable that mode by simply *not specifying*:

```yaml
  --receive.local-endpoint=RECEIVE.LOCAL-ENDPOINT
                                 Endpoint of local receive node. Used to
                                 identify the local node in the hashring
                                 configuration.
```

We can call this mode a "**Routing Receiver**". Similarly, we can skip specifying any hashring to Thanos Receiver (`--receive.hashrings-file=<path>`), explicitly purposing it only for ingesting. We can call this mode "**Ingesting Receiver**".

User can also mix all of those two modes for various federated hashrings etc. So instead of what we had before:

![Before](https://docs.google.com/drawings/d/e/2PACX-1vTfko27YB_3ab7ZL8ODNG5uCcrpqKxhmqaz3lW-yhGN3_oNxkTrqXmwwlcZjaWf3cGgAJIM4CMwwkEV/pub?w=960&h=720)

We have:

![After](https://docs.google.com/drawings/d/e/2PACX-1vTVrtCGjR4iMbrU7Kj6QAn1a1m4fr-kvoQVDAK4lzQ_wWfXfpLLEE9HB948-WHI5ZG6s1iGWt51R593/pub?w=960&h=720)

This allows us to (optionally) model deployment in a way that avoid expensive re-configuration of the stateful ingesting Receivers after the hashring configuration file has changed.

In comparison to previous proposal (as mentioned in [alternatives](#previous-proposal-separate-receive-route-command) we have big advantages:

1. We can *reduce number of components* in Thanos system, we can reuse similar component flags and documentation. Users has to learn about one less command and in result Thanos design is much more approachable. Less components mean less maintainance, code and other implicit duties: Separate changelogs, issue confusions, boilerplates, etc.
2. Allow consistent pattern with Query. We don't have separate StoreAPI component for proxying, we have that baked into Querier. This has been proven to be flexible and understandable, so I would like to propose similar pattern in Receiver.
3. This is more future proof for potential advanced cases like *chain of routers -> receivers -> routers -> receivers* for federated writes, so ***trees with depth n***.

### Plan

* Receiver without `--receive.hashrings` does not forward or replicate requests, **it routes straight to multi-tsdb**.
* Receiver without ` --Receiver.local-endpoint` will assume that no storage is needed, **so will skip creating any resources for multi TSDB**.
* Add changes to the documentation (it's simplistic now). Mention two modes.

### Alternative Solutions

#### Previous Proposal: Separate receive-route command

1. Split the Receiver component into **receive-route** and **receiver** (and ensure ease of resharding events).
2. Evaluate any effects on performance by simulating scenarios and collecting and analyzing metrics.
3. Use ***consistent hashing*** to avoid reshuffling time series after resharding events. The exact consistent hashing mechanism to be used needs some further research.
4. **Migration**: We document how the new architecture can be set up to have the same general deployment of the old architecture. (We run router and Receiver on the same node).

This potentially makes the receiver more difficult to operate and understandable for Thanos users. I would argue this is however much harder in overall Thanos deployment. Otherwise, this option is exactly the same.

#### Flag for current Receiver: --receive-route

Idea would be similar same as in [Proposal](#proposal), but there will be explicit flag to turn off local storage capabilities.

I think we can have much more understandable logic if *we simply not* configure hashring for **ingesting receivers** and not configure local hashring endpoint to notify that such Receiver instance will never store anything.
