# Deployment

## Overview

Production deployment target is Kubernetes (EKS), reconciled via GitOps with Argo CD, with cloud infrastructure provisioned by Terraform. The actual day-to-day local deployment mechanism remains Docker Compose (see `docker-compose.yml`, `deployments/docker/`) — the EKS path described here is the portfolio/production target being built out, not a replacement for local dev.

Deployment Strategy:

- **Docker images** — a single shared image (`deployments/docker/app/Dockerfile`) for producer/consumer/health-server/schema-apply, and a KRaft-mode Kafka image (`deployments/docker/kafka/Dockerfile`) for the StatefulSet + topic-creation Job.
- **Terraform** (`deployments/terraform/`) — provisions VPC/EKS only (see "Architecture / Strategy" comparison below for why there's no MSK or ClickHouse-EC2 module).
- **CI** (`.github/workflows/release.yml`) — builds/pushes images to ECR and promotes the tag into `nyc-taxi-gitops`.
- **`nyc-taxi-gitops`** — holds the Kubernetes manifests for producer/consumer/health-server, the Kafka and ClickHouse StatefulSets, monitoring, and Argo CD's app-of-apps.

Repository layout is a 2-repo split:

```
nyc-taxi                          # this repo: app code + Terraform infrastructure
    ├── etl/                              # producer, consumer, domain pipeline
    ├── clickhouse/                       # schema, migrations, materialized views
    ├── deployments/
    │   ├── docker/
    │   │   ├── app/                      # shared image: producer/consumer/health-server/schema-apply
    │   │   ├── kafka/                    # KRaft broker image + kafka-topics.sh (also used for the topic Job)
    │   │   └── clickhouse/               # ClickHouse image (used both by Compose and the EKS StatefulSet)
    │   └── terraform/                    # AWS infra: VPC + EKS only (Kafka/ClickHouse run as StatefulSets, see below)
    │       ├── modules/{networking,iam,eks}
    │       └── environments/{dev,staging,prod}/{backend.hcl,terraform.tfvars}
    └── .github/workflows/
        ├── ci.yml                        # lint, typecheck, unit tests, build check -- every push/PR
        └── release.yml                   # build+push images, promote tag -- push to main only

nyc-taxi-gitops                   # separate repo: Kubernetes manifests only (manifests below not yet scaffolded)
    ├── bootstrap/                        # Argo CD install + AppProject + root app-of-apps
    ├── argocd/                           # one Application per service
    └── kubernetes/
        ├── namespaces/
        ├── producer/, consumer/, health-server/   # base + overlays/{dev,staging,prod}
        ├── kafka/                        # KRaft StatefulSet + topic-creation Job
        ├── clickhouse/                   # StatefulSet + schema-apply Job
        ├── monitoring/                   # Prometheus + Grafana (ConfigMap-provisioned dashboards)
        └── ingress/
```

Argo CD is pointed only at `nyc-taxi-gitops`. It never reads from `nyc-taxi` directly.

## CI/CD flow

Three separate workflows, deliberately kept apart:

- **`ci.yml`** — runs on every push and PR, any branch. `lint` (ruff), `typecheck` (mypy --strict), `unit-tests` (pytest, `--cov-fail-under=80`), `build` (wheel build + import smoke test). No AWS access, no cluster access — pure correctness gate.
- **`integration.yml`** — runs on push/PR to `main`, plus manual `workflow_dispatch` (with a `debug` input to bump log level). Brings up the full local stack via `docker compose up -d`, waits for ClickHouse and Kafka to be ready, applies the schema (`scripts/apply_schema.sh`) and topics (`scripts/create_topics.sh`), runs a smoke test, then `pytest tests/integration/ -m integration`. On failure it dumps ClickHouse/Kafka logs and any rejected-record files before tearing the stack down (`docker compose down --volumes`, always). This is the one CI job that actually exercises Kafka + ClickHouse end-to-end — `ci.yml`'s unit tests never touch real infrastructure.
- **`release.yml`** — runs only on push to `main`. This is the one that talks to AWS:

```
push to main
    │
    ▼
changes job: dorny/paths-filter detects which image contexts changed
    │
    ├── app paths changed (etl/**, pyproject.toml, data/reference/**,
    │   clickhouse/migrations/**, deployments/docker/app/**)?
    │       → build-app: OIDC-assume AWS_ECR_PUSH_ROLE_ARN → ECR login →
    │         docker build/push (tag = short SHA + latest)
    │
    ├── kafka paths changed (deployments/docker/kafka/**)?
    │       → build-kafka: same OIDC/ECR/build/push pattern
    │
    ▼
promote job (runs if either build succeeded)
    │
    ▼
checkout nyc-taxi-gitops (GITOPS_DEPLOY_TOKEN) → kustomize edit set image
    → commit + push as nyc-taxi-release-bot
    │
    ▼
Argo CD detects the change in nyc-taxi-gitops → reconciles the EKS cluster
```

Path filtering means an app-only commit never triggers a Kafka image rebuild and vice versa; a Terraform-only commit triggers neither (Terraform isn't built as an image). AWS authentication is OIDC federation (`aws-actions/configure-aws-credentials@v4` assuming `AWS_ECR_PUSH_ROLE_ARN`) — no static AWS keys stored in the repo.

**Terraform apply is currently manual, not CI-driven.** `deployments/terraform/` is `terraform validate`-clean but there is no `terraform plan`/`apply` CI job yet — provisioning the actual EKS cluster is a manual `terraform apply -var-file=... -backend-config=...` run from a machine with AWS credentials. Wiring this into CI is a candidate for later, once the cluster exists and a Terraform Cloud/Atlantis-style gated-apply workflow makes sense (see "When to revisit" below). One manual follow-up after apply: paste the `alb_controller_role_arn` output into `nyc-taxi-gitops`'s `aws-load-balancer-controller` Application (`serviceAccount.annotations`) — there's no automated handoff between the two repos for this IRSA role.


## Why self-hosted Kafka + ClickHouse on EKS here, not MSK + ClickHouse-on-EC2

`docs/scaling_notes.md`'s "AWS Production Architecture" section documents a *different* stack: Amazon MSK for Kafka and ClickHouse on dedicated EC2 (NVMe RAID-0, Keeper-based replication). That remains the honest answer for what a real-production deployment of this pipeline would use — it is **not** superseded by what's built here.

What `deployments/terraform/` and the planned `nyc-taxi-gitops` manifests build instead — call it **Option A** — runs Kafka (KRaft mode) and ClickHouse as Kubernetes StatefulSets inside the same EKS cluster as the app, with PersistentVolumeClaims backed by the EBS CSI driver. The MSK/EC2 design from `scaling_notes.md` — call it **Option B** — was deliberately not implemented here.

| | Option A — self-hosted on EKS (built here) | Option B — MSK + ClickHouse-on-EC2 (`scaling_notes.md`) |
|---|---|---|
| Kafka | KRaft StatefulSet, PVC-backed, manual partition/broker scaling | Amazon MSK — AWS operates brokers, patching, replication |
| ClickHouse | StatefulSet, PVC-backed (EBS, not NVMe) | Dedicated EC2 (`r6i.4xlarge`, NVMe RAID-0), Keeper-based replication |
| Storage locality | EBS over network — no local NVMe passthrough for StatefulSet pods | NVMe instance storage, directly attached |
| Rescheduling a broker/node | Pod reschedules, PVC reattaches — some latency, but automatic | Manual/ASG-driven EC2 replacement, more operational care |
| Operational burden | You own Kafka/ClickHouse upgrades, backups, tuning inside k8s | AWS owns Kafka operations (MSK); ClickHouse ops still manual but isolated from k8s |
| AWS surface exercised | EKS, IRSA/OIDC, StatefulSets, PVCs, EBS CSI, Kustomize, Argo CD | MSK, IAM auth for Kafka, EC2 fleet management, cross-AZ replication |
| Cost profile | Cheaper at small scale — one cluster, no MSK/EC2 premium | Higher baseline cost (MSK + large EC2 instances) |

**Why Option A was chosen for this repo:** this is a learning/portfolio deployment, not a production system serving real traffic or an SLA. The goal is to maximize hands-on Kubernetes/AWS surface area — StatefulSets, PVC lifecycle, IRSA, Kustomize overlays, Argo CD reconciliation — which Option B mostly abstracts away behind managed services (MSK in particular). Option A is also materially cheaper to run for a demo cluster, since it avoids MSK's and large-EC2's baseline cost. `single_nat_gateway = true` and SPOT node capacity in `deployments/terraform/` follow the same reasoning — deliberate cost-over-availability trades appropriate for a non-production cluster.

This is a deliberate divergence, not a correction: `docs/scaling_notes.md` is kept exactly as it is, documenting what a real-production deployment of this pipeline should use. If this project ever needed to actually serve production traffic, the honest move would be migrating to Option B, not scaling Option A further — self-hosting stateful infra inside Kubernetes trades real production capability for what's usually a bad trade at scale, made deliberately here for its learning value instead.


## Why a 2-repo split (not 3)

The alternative considered was three repos: `nyc-taxi` (app only), `nyc-taxi-manifests`, `nyc-taxi-infrastructure` (Terraform separate from app code). The question is not whether to separate manifests from app code — that separation is kept in both designs — but whether Terraform deserves a repo of its own.

### The non-negotiable separation: build-time vs. deploy-time

Argo CD needs a repo it polls and reconciles against that is decoupled from application CI. This is kept in the 2-repo design: `nyc-taxi-gitops` is the only repo Argo CD watches, and nothing in `nyc-taxi`'s CI has cluster credentials — it only opens a commit/PR against the manifests repo. This gives:

- A broken or compromised app pipeline cannot push directly to the cluster; it can only propose a manifest change.
- A clean audit trail of "what is actually running" as a linear commit history in one repo, independent of how many services eventually exist.
- Room to grow `nyc-taxi-gitops` into a shared deploy repo for additional services later without touching the app repo's structure.

### Why Terraform stays with the app code here

The 3-repo model earns its overhead once infrastructure and application ownership genuinely diverge. That divergence doesn't exist yet for this project:

| Factor | 3-repo case for splitting Terraform out | Current reality for nyc-taxi |
|---|---|---|
| Shared infra across services | Multiple apps depend on the same Terraform-managed VPC/cluster/data stores | Single service (producer + consumer); Terraform in this repo provisions the VPC/EKS cluster only this service uses |
| Distinct approval gates | A platform/infra team reviews infra changes under different rules than app reviewers | One team/owner reviews both |
| Decoupled apply workflow | Terraform applies run through a separate tool (e.g. Atlantis) with its own PR-driven process | Terraform applies are run manually today; if automated, they can share `nyc-taxi`'s CI, gated by a manual approval step on `plan` |
| Change coupling | Infra and app code evolve independently, on different cadences | They frequently change together — a new `.env` setting often means a new Kubernetes ConfigMap key or a new Kafka topic *in the same PR* as the code that reads it |

Keeping Terraform in `nyc-taxi` means a change like "add a new Kafka topic" is one PR — the `kafka-topics.sh` update and the consumer code that reads from it are reviewed and merged together (the topic itself is created by a Job in `nyc-taxi-gitops`, not by Terraform, since Kafka runs in-cluster — see "Why self-hosted" above). Splitting Terraform into its own repo would turn coupled changes into multiple PRs that have to be coordinated and merged in the right order, without buying any real isolation benefit at this scale.

Fewer repos also means less standing overhead for a small team: one CI configuration to maintain instead of two, one access-control policy, one place to look when tracing how an infra change and the code that depends on it landed together.

### Cost of this choice

CI in `nyc-taxi` triggers conditionally on changed paths (see "CI/CD flow" above) — `release.yml`'s `build-app`/`build-kafka` jobs only run when their respective paths change. Without path filtering, every commit would rebuild both images regardless of what actually changed. This is a CI configuration cost, not an architectural one.


## When to revisit: split Terraform into its own repo

Move `deployments/terraform/` out into a dedicated `nyc-taxi-infrastructure` repo when any of the following becomes true:

1. **A second service is added** that provisions against or shares the same Terraform-managed infrastructure (the EKS cluster, its VPC). At that point Terraform is no longer "this service's infra" — it's platform infra multiple app repos depend on.
2. **A dedicated infra/platform owner emerges** who needs a review gate on infra changes that's independent of app code review.
3. **Terraform applies move to a tool with its own PR-driven workflow** (e.g. Atlantis, Terraform Cloud VCS-driven runs) that expects to own a repository's `plan`/`apply` lifecycle rather than sharing CI with app builds.

Separately, **revisit Option A vs. Option B** (see above) if this deployment ever needs to handle real production traffic or an SLA — at that point the operational cost of self-hosting Kafka/ClickHouse inside Kubernetes stops being a reasonable trade, and migrating to `scaling_notes.md`'s MSK + ClickHouse-on-EC2 design is the honest move, not scaling the in-cluster StatefulSets further.

The Terraform-repo extraction itself is mechanical: move `deployments/terraform/` to the new repo, point its CI at cloud credentials, and leave `nyc-taxi-gitops` untouched — Argo CD's view of the world doesn't change either way.


## Repository responsibilities

| Repo | Owns | CI does | Has cluster/cloud credentials? |
|---|---|---|---|
| `nyc-taxi` | App code (`etl/`), ClickHouse schema/migrations, Docker images, Terraform (`deployments/terraform/`) | `ci.yml`: lint, typecheck, unit tests, build check. `integration.yml`: full Kafka+ClickHouse integration tests against Docker Compose. `release.yml`: build & push app/kafka images (path-filtered), promote image tag to `nyc-taxi-gitops` | OIDC-assumed AWS role for ECR push only (`release.yml`) — no Kubernetes/cluster access. Terraform applies are manual, not run from CI. |
| `nyc-taxi-gitops` | Kubernetes manifests, Argo CD `Application` definitions | Validate manifests (e.g. `kubeconform`, `kustomize build`) on PR — not yet implemented | None — Argo CD pulls, nothing pushes to the cluster from here |

Argo CD itself holds the only credentials that can change what's running on the cluster, and it only ever reads from `nyc-taxi-gitops`.


## Environments

Environment separation (dev/staging/prod) is expressed on both sides of the split:

- **`nyc-taxi`** — Terraform uses separate state per environment via a partial S3 backend: `deployments/terraform/environments/{dev,staging,prod}/backend.hcl` (state bucket/key/DynamoDB lock table) and `terraform.tfvars` (node count, instance type, capacity type — e.g. SPOT for dev/staging, ON_DEMAND for prod). A `plan`/`apply` against one environment cannot touch another's state, since each has a distinct S3 key.
- **`nyc-taxi-gitops`** — Kustomize overlays per service: `kubernetes/{producer,consumer,health-server}/overlays/{dev,staging,prod}/`, each with its own Argo CD `Application` pointing at the same base manifests with environment-specific values (replica count, resource limits). The repo exists; these manifests are the remaining scaffolding work.
