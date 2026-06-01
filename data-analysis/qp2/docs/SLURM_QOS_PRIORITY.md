# Slurm QOS Priority — Web vs Live Jobs

## Overview

Web-submitted reprocess jobs (from the data portal) run at a lower Slurm
priority than jobs submitted by the live data collection pipeline. This
prevents a flood of manual reprocess requests from delaying automated
processing during active beamline operation.

---

## How it works

### Code side (already deployed)

`run_command()` in `qp2/image_viewer/utils/run_job.py` accepts an optional
`qos` parameter. When set, it adds `#SBATCH --qos={qos}` to the sbatch
script. When omitted or `None`, no QOS directive is written and the job
inherits the partition/account default.

```python
# Signature
def run_command(..., qos: Optional[str] = None, ...):
    ...
    if qos:
        script_content += f"#SBATCH --qos={qos}\n"
```

The web reprocess endpoint (`reprocess_routes.py`) injects `slurm_qos='web'`
into every job payload it sends to the data processing server:

```python
payload = {
    ...
    "slurm_qos": "web",   # lower Slurm priority vs live collection jobs
}
```

This value flows through `analysis_manager.handle_external_job_request()` →
`submit_plugin_job()` → worker `self.kwargs` → `run_command(qos=self.kwargs.get('slurm_qos'))`.

**Existing callers (live collection jobs) are unaffected.** `dict.get()` returns
`None` when `slurm_qos` is absent from kwargs, and `run_command` skips the QOS
directive when `qos` is `None`.

Workers already patched:

| Worker | File |
|--------|------|
| XDS | `image_viewer/plugins/xds/submit_xds_job.py` |
| nXDS | `image_viewer/plugins/nxds/submit_nxds_job.py` |
| CrystFEL | `image_viewer/plugins/crystfel/submit_crystfel_job.py` |
| AutoPROC / Xia2 / Dimple | `pipelines/autoproc_xia2/pipeline_runners.py` |

---

## Status

| Component | Status |
|-----------|--------|
| `run_job.py` — `qos` parameter | ✅ Done (commit 4a94a06b) |
| Worker files (xds/nxds/crystfel/autoproc/xia2) — pass `slurm_qos` | ✅ Done |
| `reprocess_routes.py` — inject `slurm_qos` via `QP2_SLURM_WEB_QOS` env var | ✅ Done (inactive until env var set) |
| Slurm `slurm.conf` — priority + preemption | ⏳ Pending (see below) |
| `sacctmgr` — create QOS tiers | ⏳ Pending |

The code changes are **already deployed and safe to run without the Slurm
configuration**. The `slurm_qos` field is only injected into the job payload
when the environment variable `QP2_SLURM_WEB_QOS` is set on the API server.
Until then, reprocess jobs submit to Slurm with no QOS directive and behave
exactly as before.

**To activate priority after completing the Slurm setup below:**

```bash
# Add to the API server's environment (e.g. /etc/systemd/system/qp2-web-backend.service)
Environment=QP2_SLURM_WEB_QOS=web
sudo systemctl daemon-reload && sudo systemctl restart qp2-web-backend
```

---

## One-time Slurm configuration (requires slurmctld restart)

Perform off-hours. The restart takes ~10 seconds and does not kill running jobs.

### 1. Edit `/etc/slurm/slurm.conf`

Add these four lines anywhere in the file:

```conf
PriorityType=priority/multifactor
PriorityWeightQOS=1000
PreemptType=preempt/qos
PreemptMode=SUSPEND,GANG
```

`PreemptType=preempt/qos` allows higher-QOS jobs to preempt lower-QOS jobs.
`PreemptMode=SUSPEND,GANG` pauses the web job while the live job runs, then
resumes it automatically — no work is lost and the user sees no error.

> **Without preemption:** priority only controls queue order. If web jobs have
> already taken all nodes, a live job still waits until a node frees naturally.
> With preemption, a live job immediately displaces a running web job.

### 2. Restart slurmctld

```bash
sudo systemctl restart slurmctld
```

### 3. Create QOS tiers

Run once on the Slurm controller (bl1ws3):

```bash
# High-priority QOS for live collection pipeline jobs (default)
sacctmgr add qos live Priority=1000 Flags=NoDecay

# Low-priority QOS for web-submitted reprocess jobs
sacctmgr add qos web  Priority=100  Flags=NoDecay

# Make both QOS available to the cluster
sacctmgr modify cluster cluster set qos=live,web

# Set 'live' as the default QOS for the main partition so all existing
# jobs automatically inherit high priority without any code changes
sacctmgr modify partition main set defaultqos=live

# Allow 'live' jobs to preempt (suspend) running 'web' jobs
sacctmgr modify qos web set Preempt=live
```

### 4. Verify

```bash
# Check QOS definitions
sacctmgr show qos format=name,priority,flags

# Check a submitted web reprocess job shows QOS=web
squeue -o "%.18i %.9P %.20j %.8u %.8T %.10M %.9l %Q" | grep web
```

---

## Priority ratio

| Job source | QOS | Priority |
|------------|-----|----------|
| Live collection pipeline (default) | `live` | 1000 |
| Web reprocess (data portal) | `web` | 100 |

The 10:1 ratio means live jobs jump ahead of queued web jobs. With
`PreemptMode=SUSPEND,GANG` configured, a live job that needs resources will
also suspend any running web job, freeing the node immediately. The web job
resumes automatically once the live job completes — no work is lost.

Idle nodes are still used immediately by web jobs when no live jobs are
waiting (`OverSubscribe=YES` on `main` partition preserves this behaviour).

### Preemption behaviour summary

| Scenario | Result |
|----------|--------|
| Web job queued, live job submitted | Live job runs first (priority ordering) |
| Web job running, live job submitted, node needed | Web job suspended; live job runs; web job resumes |
| Web job running, live job submitted, spare node available | Both run concurrently; no preemption |
| Live job running | Never preempted by web jobs |

---

## Extending to other low-priority job types

To mark any other job type as low priority, pass `slurm_qos='web'` (or a
new QOS name) through the job kwargs and ensure `run_command()` receives it
via the `qos=` parameter. No Slurm config changes are needed for additional
job types once the QOS tiers exist.

---

## Rollback

**Disable preemption only** (no restart needed):
```bash
sacctmgr modify qos web set Preempt=''
```

**Disable priority ordering only** (no restart needed):
```bash
sacctmgr modify qos live set Priority=100   # equal to web
```

**Remove everything** — revert `slurm.conf` to remove the four added lines and restart:
```bash
sudo systemctl restart slurmctld
sacctmgr delete qos live
sacctmgr delete qos web
```
