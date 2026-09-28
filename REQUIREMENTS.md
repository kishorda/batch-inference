# Requirements: Hybrid Scheduling & Queueing Layer for Serverless Batch Inference

**Status:** Draft v0.1 · **Date:** 2026-09-28 · **Author:** Kishor Aher

**Source:** [Scheduling and Queueing at Scale: The Hardest Problem in Serverless Batch Inference](https://kishordaher.wordpress.com/2026/09/28/scheduling-and-queueing-at-scale-the-hardest-problem-in-serverless-batch-inference/)

---

## 1. Purpose & Scope

This document specifies the scheduling and queueing layer for a serverless batch inference platform on Kubernetes (KServe, Ray, or a homegrown stack). The platform accepts batches of inference work, provisions capacity for them, runs the model, releases the capacity, and charges only for what was used.

Request-time serving optimizes for latency. This layer optimizes for **throughput and cost** across a heterogeneous GPU fleet shared by many tenants.

**Target architecture: hybrid.** A fixed batch pool is sized to a baseline SLA. When the pool saturates, work overflows into a shared P1 queue that runs on real-time serving infrastructure, behind real-time (P0) traffic. The hybrid inherits the failure modes of both the fixed-pool and the shared-queue architectures, and each of those failure modes is covered by at least one requirement below (see §7).

**In scope:** job admission, quotas, fairness, placement, hybrid routing, capacity signals and autoscaling coordination, preemption, resumability, and observability.

**Out of scope:** the inference runtime itself, model packaging, the API gateway beyond how it hands off to the scheduler, and billing (except for emitting GPU-minute usage).

### Priority keywords

- **MUST:** required for launch.
- **SHOULD:** strongly expected. Skipping one needs a documented reason.
- **MAY:** optional or deferred.

### Source tags

Each requirement names the section of the blog post it comes from:

| Tag | Section of the post |
|---|---|
| `§Why` | Why batch inference scheduling is a different problem |
| `§Fixed` | 1. Fixed capacity, scaled as a unit |
| `§Shared` | 2. Shared priority queue, real-time as P0 |
| `§Hybrid` | Closing discussion: neither architecture is strictly better, and the hybrid |
| `§Works` | What tends to actually work |
| `§Tradeoff` | The uncomfortable trade-off |

---

## 2. Glossary

| Term | Definition |
|---|---|
| **P0** | Real-time serving traffic. Highest priority. May preempt P1. |
| **P1** | Batch work running on shared real-time infrastructure. Lower priority than P0. |
| **Fixed pool** | Dedicated batch capacity that is scaled up or down as one unit. |
| **Overflow / spill** | Routing batch work from a saturated fixed pool into the shared P1 queue. |
| **Admission** | Deciding whether a job may consume cluster resources *now*: quotas, fairness, priority. |
| **Placement** | Deciding *which node(s)* an admitted job runs on: bin-packing and topology. |
| **Gang scheduling** | All-or-nothing placement. A multi-GPU job gets every GPU it needs at once, or none of them. |
| **Fragmentation** | Free capacity that no pending job can use because of its shape or topology, e.g. three nodes each with one free GPU when a job needs two GPUs on the same NVLink domain. |
| **Schedulable capacity** | Free capacity that can actually satisfy a pending job's shape and topology constraints. |
| **Resumability** | Whether a job can continue from a checkpoint after interruption, and how much progress it has made. |
| **Virtual-time fair scheduling** | Fair-share scheduling that orders work by each tenant's normalized consumption (e.g. WFQ, DRF-style). |
| **GPU-minute** | The unit of accelerator cost: one GPU of a given type held for one minute. |
| **Cold start** | Time from a provisioning decision until a node is ready to run a job (boot, drivers, image pull, model load). |

---

## 3. Workload Assumptions

| ID | Assumption | Source |
|---|---|---|
| WA-1 | Jobs have heterogeneous resource shapes, from 8×H100 for large models down to 1×RTX 6000 Pro. Nodes are not interchangeable. | §Why |
| WA-2 | Jobs run for minutes to hours. A bad placement holds an expensive node for the job's whole runtime. | §Why |
| WA-3 | Jobs arrive in bursts (end-of-day exports, retraining triggers, downstream pipeline kickoffs), not as a steady stream. | §Why |
| WA-4 | Many tenants submit work, with different SLAs and job classes. | §Why |
| WA-5 | The system runs thousands of concurrent jobs across the fleet. | Intro |
| WA-6 | A GPU node takes minutes to boot. | §Works |
| WA-7 | Spot/preemptible capacity may be part of the cost model. | §Works |
| WA-8 | Real-time serving has provisioned capacity that sits idle overnight and between peaks. | §Shared |

---

## 4. Architecture Constraints

| ID | Constraint | Pri | Acceptance criterion | Source |
|---|---|---|---|---|
| AC-1 | Admission (quotas, fairness, priority) and placement (bin-packing, topology) shall be separate components with a documented interface between them. | MUST | Either component can be replaced or tested alone. Placement never reads quota state, and admission never reads node topology. | §Works |
| AC-2 | The system shall implement the hybrid topology: a fixed batch pool, plus overflow into a shared P1 queue on real-time infrastructure. | MUST | Jobs can be observed running in both the pool and P1 overflow, and routing between them follows FR-HYB. | §Hybrid |
| AC-3 | The scheduling layer shall sit on top of, or replace, the default Kubernetes scheduler for batch workloads. The default scheduler's generic bin-packing shall not be the only placement logic. | MUST | Batch pods are placed by the batch scheduler (e.g. custom scheduler name, scheduler plugin, or gang-aware controller). | §Why |
| AC-4 | The admission/placement split (AC-1) shall be built in Phase 1, before the more advanced features, so it doesn't require major surgery on a live platform later. | MUST | Phase 1 deliverable (see §8). | §Tradeoff |

---

## 5. Functional Requirements

> **Note on numeric targets:** the numbers in §5 and §6 are provisional defaults chosen as reasonable starting points for a multi-tenant GPU fleet. The source post does not specify them. Revisit them against real workload and fleet data before treating them as commitments (see §9).

### 5.1 Job Submission & Job Model (FR-JOB)

| ID | Requirement | Pri | Acceptance criterion | Source |
|---|---|---|---|---|
| FR-JOB-1 | A job submission shall declare its resource shape: GPU type (or acceptable set of types), GPU count, CPU and memory, and interconnect requirements (e.g. same-node NVLink). | MUST | A submission without a resource shape is rejected, and every declared field is visible to placement. | §Why |
| FR-JOB-2 | A job shall carry a tenant ID, a job class, and an SLA class (e.g. urgent, standard, best-effort). | MUST | The fields are required and are used by admission. | §Why |
| FR-JOB-3 | A job shall declare its resumability: whether it checkpoints, its checkpoint interval or strategy, and where checkpoints are stored. | MUST | The field is required. A job with no declaration is treated as non-resumable. | §Shared, §Works |
| FR-JOB-4 | A job may declare a model identifier for affinity placement (FR-PLC-5). | SHOULD | Placement can read the model ID. | §Why |
| FR-JOB-5 | The system shall return a stable job ID and expose a status lifecycle: `Submitted → Queued → Admitted → Placed → Running → (Preempted → Queued)* → Succeeded / Failed / Cancelled`. | MUST | Every transition is timestamped and can be queried through the API. | — |
| FR-JOB-6 | A job shall report progress (e.g. percent complete, or items processed out of total) and the location of its latest valid checkpoint. | MUST | The scheduler can read up-to-date progress for any running resumable job. | §Shared |

### 5.2 Admission: Quotas, Priority & Fairness (FR-ADM)

| ID | Requirement | Pri | Acceptance criterion | Source |
|---|---|---|---|---|
| FR-ADM-1 | Admission shall enforce per-tenant quotas (concurrent GPUs by type, GPU-minutes per window). | MUST | A tenant at quota has new jobs held in Queued, not rejected or placed. | §Works |
| FR-ADM-2 | Admission shall enforce strict priority between classes: P0 over P1, and pool-urgent over pool-standard. | MUST | A lower-class job is never admitted ahead of a higher-class job for the same capacity. | §Shared |
| FR-ADM-3 | *Within* each priority class, admission shall use multi-tenant fair-share scheduling (weighted fair queueing or virtual-time fair scheduling), not FIFO. | MUST | A test where tenant A submits 10,000 small jobs and then tenant B submits 5 urgent jobs: B's jobs are admitted within the starvation bound (NFR-4), independent of A's backlog. | §Shared, §Works |
| FR-ADM-4 | Admission shall support sub-priority classes inside P1 and inside the pool. | SHOULD | At least three sub-priorities can be configured, and ordering follows them. | §Shared |
| FR-ADM-5 | Tenant weights for fair share shall be configurable without restarting the scheduler. | SHOULD | A weight change takes effect on the next scheduling cycle. | §Works |
| FR-ADM-6 | Fairness shall be enforced in the scheduler, where placement decisions are made. Rate limiting at the API gateway may exist but shall not be the fairness mechanism. | MUST | With gateway rate limits disabled, FR-ADM-3 still passes. | §Works |
| FR-ADM-7 | Admission shall only admit a job when placement confirms schedulable capacity, not just free capacity (see FR-PLC-3). | MUST | No admitted job sits Pending on a missing GPU for longer than 60 seconds. | §Fixed |

### 5.3 Placement: Bin-packing & Topology (FR-PLC)

| ID | Requirement | Pri | Acceptance criterion | Source |
|---|---|---|---|---|
| FR-PLC-1 | Placement shall be topology-aware: NUMA, GPU interconnect (NVLink/NVSwitch domains), and node locality are inputs to every decision. | MUST | A job that needs 2 NVLink-connected GPUs is never placed on 2 GPUs without NVLink between them. | §Fixed, §Works |
| FR-PLC-2 | Multi-GPU and multi-node jobs shall be gang-scheduled: all resources are bound together or none are. | MUST | No job ever holds a partial allocation (e.g. 3 of 4 GPUs) while it waits for the rest. Verified by fault injection. | §Works |
| FR-PLC-3 | Placement shall compute and expose *schedulable* capacity per job shape, separately from raw free capacity. | MUST | A metric exists for each common shape (1, 2, 4, 8 GPUs by type), and the dashboard shows the gap between free and schedulable. | §Fixed |
| FR-PLC-4 | Placement shall reduce fragmentation actively, e.g. by packing small jobs onto partially used nodes, keeping whole nodes free for large shapes, or migrating/defragmenting resumable jobs. | SHOULD | Under a mixed-shape benchmark, the fragmentation ratio (NFR-6) stays under target. | §Fixed |
| FR-PLC-5 | Placement shall prefer nodes where the job's model is already cached or warm (model affinity). | SHOULD | Affinity hit rate is measured, and warm placement beats cold placement on time-to-running. | §Why |
| FR-PLC-6 | Placement shall be cost-aware. When several node types satisfy a job, it picks the lowest GPU-minute cost that still meets the job's SLA. | SHOULD | In a benchmark, expensive accelerators are not used for jobs that fit cheaper ones while cheaper ones are available. | §Why |
| FR-PLC-7 | Placement shall avoid leaving expensive accelerators idle because of naive packing. | MUST | Idle-accelerator ratio stays within NFR-5 while there are queued jobs that fit. | §Why |

### 5.4 Hybrid Routing: Pool ↔ Overflow (FR-HYB)

| ID | Requirement | Pri | Acceptance criterion | Source |
|---|---|---|---|---|
| FR-HYB-1 | Jobs shall go to the fixed pool by default and spill to the shared P1 queue when the pool is saturated for their shape. | MUST | With the pool saturated for shape S, the next job of shape S is placed through P1 when shared capacity allows. | §Hybrid |
| FR-HYB-2 | Spill policy shall be configurable per job class and tenant (never spill, spill after N seconds queued, spill immediately). | MUST | Each policy is demonstrated in a test. | §Hybrid |
| FR-HYB-3 | SLA-critical job classes shall be able to run pool-only, so they are never exposed to P0 preemption. | MUST | A pool-only job is never placed on shared infrastructure. | §Hybrid |
| FR-HYB-4 | Only resumable jobs (FR-JOB-3), or jobs short enough that a restart costs little, shall be eligible to spill to P1 by default. | SHOULD | A non-resumable long job does not spill unless its policy overrides this. | §Shared |
| FR-HYB-5 | Fair-share accounting (FR-ADM-3) shall be unified across pool and overflow, so a tenant can't get extra share by spilling. | SHOULD | A tenant's consumption across both paths feeds one fair-share state. | §Works |

### 5.5 Capacity Signals & Autoscaling (FR-CAP)

| ID | Requirement | Pri | Acceptance criterion | Source |
|---|---|---|---|---|
| FR-CAP-1 | Queue depth, broken down by shape and class, shall be a first-class scaling signal for the fixed pool, in addition to pending-pod count. | MUST | The autoscaler consumes a queue-depth metric, and scale decisions can be traced back to it. | §Works |
| FR-CAP-2 | The pool shall scale ahead of demand using a forecast from queue trends (at least a moving average) and pre-warm nodes before jobs are dequeued. | MUST | In a burst replay, provisioning starts before the first job of the burst is dequeued, and lead time ≥ 5 minutes (covering typical GPU node boot, driver init and image pull). | §Fixed, §Works |
| FR-CAP-3 | The batch scheduler and the cluster autoscaler shall share one model of near-term demand: a common forecast plus scheduler intent (jobs about to be held or released). | MUST | In a thrash test, no node is scaled down within 15 minutes before a forecast burst needs it, and no node is scaled up for jobs the scheduler is deliberately holding. | §Fixed |
| FR-CAP-4 | The pool shall support scale-to-zero, with a configurable minimum warm floor per shape and class to bound cold-start exposure. | MUST | The pool reaches zero when idle (floor = 0), and the floor is honored when set. | §Fixed |
| FR-CAP-5 | Scale-down shall be damped (cool-down windows and hysteresis) so bursty arrivals don't cause oscillation. | MUST | Under a periodic-burst workload, scale events per hour stay at or below 6 per node group. | §Fixed |
| FR-CAP-6 | The system shall detect cold-start amplification (many nodes provisioning at once while queue depth grows) and alert on it. | SHOULD | An alert fires in the 10× burst test when it is run without pre-warming. | §Fixed |
| FR-CAP-7 | P1 overflow shall not run its own node autoscaling. It uses capacity the real-time autoscaler provides. | MUST | No batch-initiated scale-up happens on shared infrastructure. | §Fixed |

### 5.6 Preemption & Resumability (FR-PRE)

| ID | Requirement | Pri | Acceptance criterion | Source |
|---|---|---|---|---|
| FR-PRE-1 | P0 demand shall be able to preempt or delay P1 jobs on shared infrastructure. | MUST | During a P0 spike, P1 pods are evicted within 30 seconds, including the checkpoint grace period (FR-PRE-4), and P0 meets its latency SLO. | §Shared |
| FR-PRE-2 | Before choosing a victim, the scheduler shall know each candidate's resumability, current progress, and checkpoint freshness. | MUST | Every preemption decision logs these inputs, and no decision is made with them missing. | §Shared, §Works |
| FR-PRE-3 | Victim selection shall minimize lost work, estimated as GPU-minutes since the last checkpoint, weighted by job class. | MUST | In a simulation, lost GPU-minutes per preemption are ≤ 50% of what random selection loses. | §Shared, §Works |
| FR-PRE-4 | The scheduler shall be able to send a preemption notice (grace period) so a job can checkpoint before it is evicted. | SHOULD | A job given a notice writes a checkpoint within the grace period. | §Works |
| FR-PRE-5 | A preempted resumable job shall re-enter the queue with its progress kept and resume from its latest valid checkpoint. It shall never silently restart from zero. | MUST | For every preempted resumable job, the resumed run starts at the last checkpoint. Silent restarts count as defects. | §Shared |
| FR-PRE-6 | Job execution shall be idempotent across retries and resumes: re-processing a range writes no duplicate or corrupt output. | MUST | Kill-and-resume tests at random points produce output identical to an uninterrupted run. | §Shared |
| FR-PRE-7 | A re-queued preempted job shall be treated as partially complete, not fresh work, for queue ordering, fair-share accounting, and ETA. | MUST | The queue view shows progress for re-queued jobs, and fair share does not double-charge work already done. | §Shared |
| FR-PRE-8 | Spot/preemptible node reclamation shall go through the same checkpoint-aware path as P0 preemption. | MUST (if spot is used) | A simulated spot reclaim produces the same outcome as FR-PRE-5. | §Works |
| FR-PRE-9 | A cap on preemptions per job (anti-starvation) shall be configurable. After the cap, the job is promoted or pinned to the pool. | SHOULD | A job preempted N times is escalated automatically. | §Shared |

### 5.7 Observability (FR-OBS)

| ID | Requirement | Pri | Acceptance criterion | Source |
|---|---|---|---|---|
| FR-OBS-1 | Emit per-tenant and per-class queue wait time (p50/p95/p99). | MUST | Metric is on the dashboard. | §Shared |
| FR-OBS-2 | Emit time-to-placement (admission → bound) and placement decision latency. | MUST | Metric is on the dashboard. | §Tradeoff |
| FR-OBS-3 | Emit free vs schedulable capacity per shape, and the fragmentation ratio. | MUST | Metric is on the dashboard. | §Fixed |
| FR-OBS-4 | Emit preemption count, lost GPU-minutes, and silent-restart count (target 0). | MUST | Metric is on the dashboard. | §Shared |
| FR-OBS-5 | Emit cold-start count and duration, pre-warm hit rate, and forecast error. | MUST | Metric is on the dashboard. | §Fixed |
| FR-OBS-6 | Emit scheduler/autoscaler disagreement events (scale-up while holding, scale-down with a forecast burst coming). | SHOULD | Metric is on the dashboard. | §Fixed |
| FR-OBS-7 | Emit pool vs overflow utilization and spill rate by class. | MUST | Metric is on the dashboard. | §Hybrid |
| FR-OBS-8 | Emit GPU-minute usage per tenant and job for billing. | MUST | Totals reconcile with node usage within 1%. | Intro |
| FR-OBS-9 | For any job, the system shall explain why it is still queued (quota, fair share, no schedulable shape, waiting on a gang). | SHOULD | An API or CLI returns the blocking reason. | §Works |

---

## 6. Non-Functional Requirements

| ID | Requirement | Pri | Acceptance criterion | Source |
|---|---|---|---|---|
| NFR-1 | **Scale:** support ≥ 5,000 concurrent running jobs, ≥ 100,000 queued jobs, and ≥ 500 tenants. | MUST | Load test at target volume. | Intro |
| NFR-2 | **Placement latency budget:** p99 placement decision latency ≤ 500 ms (p50 ≤ 100 ms) at NFR-1 scale. Smarter scheduling is slower, and this budget makes that cost explicit. | MUST | Measured through FR-OBS-2 under load. | §Tradeoff |
| NFR-3 | **Graceful degradation:** at 10× baseline arrival rate, the system keeps admitting and placing work, queue wait grows in proportion to the capacity shortfall rather than collapsing, and no component crashes or deadlocks. | MUST | 10× burst replay test. | §Fixed |
| NFR-4 | **Starvation bound:** no tenant's highest-sub-priority job waits more than 60 seconds for admission because of another tenant's backlog in the same class. | MUST | Checked by the FR-ADM-3 test. | §Shared |
| NFR-5 | **Utilization:** the idle-accelerator ratio (idle GPUs while jobs that fit are queued) stays ≤ 5%. | MUST | Measured over a 7-day soak. | §Why |
| NFR-6 | **Fragmentation:** 1 − schedulable/free for the largest common shape stays ≤ 20% under a mixed-shape workload. | SHOULD | Benchmark. | §Fixed |
| NFR-7 | **Durability:** queue and job state (including progress and checkpoint pointers) survive a scheduler restart or failover with no lost or duplicated jobs. | MUST | Kill-scheduler chaos test. | §Shared |
| NFR-8 | **Availability:** the scheduler control plane is highly available (active/standby or leader election), with failover in ≤ 30 seconds, with no loss of admitted or queued jobs. | MUST | Failover test. | — |
| NFR-9 | **P0 isolation:** batch overflow does not push P0 latency SLOs out of bounds. | MUST | P0 SLO holds during mixed load. | §Shared |
| NFR-10 | **Configurability:** quotas, weights, spill policies, warm floors, and forecast parameters can be changed at runtime without redeploying. | SHOULD | Config change test. | §Works |

---

## 7. Traceability: Failure Modes & Practices → Requirements

### 7.1 Failure modes (the hybrid inherits all of them)

| Failure mode from the post | Architecture | Mitigating requirements |
|---|---|---|
| GPU fragmentation: capacity reported as available that isn't schedulable | Fixed pool | FR-PLC-1, FR-PLC-3, FR-PLC-4, FR-ADM-7, FR-OBS-3, NFR-6 |
| Cold-start amplification under queueing pressure | Fixed pool | FR-CAP-1, FR-CAP-2, FR-CAP-4, FR-CAP-6, FR-OBS-5, NFR-3 |
| Autoscaler and scheduler fighting each other (thrash) | Fixed pool | FR-CAP-3, FR-CAP-5, FR-CAP-7, FR-OBS-6 |
| Queue head-of-line blocking (one tenant's burst buries urgent work) | Shared queue | FR-ADM-3, FR-ADM-4, FR-ADM-6, FR-HYB-5, NFR-4 |
| Preemption without idempotency (silent restart from zero) | Shared queue | FR-JOB-3, FR-JOB-6, FR-PRE-2 through FR-PRE-8, FR-OBS-4, NFR-7 |
| Partial gang capture (3 of 4 GPUs held) | Both | FR-PLC-2 |
| Naive bin-packing leaves expensive accelerators idle | Both | FR-PLC-6, FR-PLC-7, NFR-5 |

### 7.2 "What tends to actually work" → requirements

| Practice | Requirements |
|---|---|
| Separate admission from placement | AC-1, AC-4 |
| Queue depth as a first-class scaling signal | FR-CAP-1, FR-CAP-2 |
| Fairness in the scheduler, not the client | FR-ADM-3, FR-ADM-6 |
| Topology as a placement input | FR-PLC-1, FR-PLC-2 |
| Design for preemption from day one | FR-JOB-3, FR-PRE-2, FR-PRE-8 |

---

## 8. Phasing

Following the post's advice to *earn* complexity rather than start with it:

| Phase | Focus | Requirements |
|---|---|---|
| **1: Foundations** | Admission/placement split, job model with resumability contract, gang scheduling, topology-aware placement, fixed pool with scale-to-zero, core metrics | AC-1–4, FR-JOB-*, FR-ADM-1/2/7, FR-PLC-1/2/3/7, FR-CAP-4/5, FR-OBS-1–4/8, NFR-7/8 |
| **2: Fairness & forecasting** | Weighted fair share within classes, queue-depth forecasting and pre-warm, scheduler/autoscaler shared demand model | FR-ADM-3–6, FR-CAP-1/2/3/6, FR-OBS-5/6/9, NFR-2/3/4 |
| **3: Hybrid overflow** | Spill to shared P1, checkpoint-aware preemption, spot support, cost-aware placement, defragmentation, model affinity | FR-HYB-*, FR-PRE-*, FR-CAP-7, FR-PLC-4/5/6, FR-OBS-7, NFR-5/6/9 |

Each phase should be gated on measured need, meaning tenant count, job volume, and observed failure modes, not on schedule alone (§Tradeoff).

---

## 9. Open Questions

1. **SLA numbers:** turnaround targets per job class, and confirmation of the provisional defaults in §5 and §6 (see the note at the top of §5), once real workload and fleet data are available.
2. **Tenant scale:** tenant count and job volume at launch and at 12 months. This decides when Phase 2 and Phase 3 are justified.
3. **Spot share:** what fraction of the fleet is spot or preemptible. This decides the priority of FR-PRE-8.
4. **Checkpoint contract:** the format, storage backend, and runtime API for progress reporting and checkpoint notices (FR-JOB-6, FR-PRE-4).
5. **Base platform:** KServe, Ray, or custom. Also, a scheduler plugin vs a separate scheduler vs a queueing controller (e.g. Kueue, Volcano, YuniKorn) as the starting point for AC-3.
6. **Pool sizing:** how the baseline SLA turns into initial fixed-pool size per GPU type.
7. **Forecast model:** whether a moving average is enough, or whether known schedules (e.g. end-of-day exports) should feed the forecast directly.
