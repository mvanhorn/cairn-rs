# cairn-rs Deployment Guide

## Quick start

```bash
git clone https://github.com/avifenesh/cairn-rs
cd cairn-rs
docker compose up --build
```

The control plane starts at **http://localhost:3000**.  
Health check: `curl http://localhost:3000/health` → `{"status":"healthy",...}`

---

## Docker Compose (recommended)

```bash
# Start (foreground)
docker compose up --build

# Start (background)
docker compose up -d --build

# Stop, keep data
docker compose down

# Stop and wipe Postgres data
docker compose down -v
```

Services started:
| Service | Port | Notes |
|---|---|---|
| `cairn` | 3000 | Control-plane HTTP API |
| `postgres` | 5432 | PostgreSQL 16 (host-accessible for inspection) |
| `valkey` | 6379 | Valkey 8 (FlowFabric state — lease, lifecycle, eligibility) |

> **Valkey / FlowFabric.** `docker compose up` now provisions Valkey 8
> alongside Postgres and wires cairn-app at `CAIRN_FABRIC_HOST=valkey`. To
> point at an external Valkey instead, override `CAIRN_FABRIC_HOST` /
> `CAIRN_FABRIC_PORT` in `.env`. In-memory mode (`--db memory`) is dev-only
> and skips Fabric entirely.

---

## Production setup with external Postgres

Skip the bundled Postgres and point cairn at your own database:

```bash
docker run -d \
  --name cairn \
  -p 3000:3000 \
  -e CAIRN_ADMIN_TOKEN="$(openssl rand -hex 32)" \
  cairn-rs \
  --mode team \
  --addr 0.0.0.0 \
  --port 3000 \
  --db "postgres://cairn:password@db.internal:5432/cairn"
```

The database is created and migrated automatically on first start.

---

## Minimum kernel version

The upcoming sandbox confinement work (F65 PR-4) requires **Linux kernel
5.13 or newer** on the host. Kernel 5.13 is the first release with
Landlock LSM, the unprivileged filesystem sandbox we use to confine
sub-agent workspace writes. Once that work lands, older kernels will
fall back to a degraded mode on boot and refuse to run confined agents.

Today cairn-rs does not yet enforce this requirement at startup — but
the kernel target is locked so that self-hosted operators can provision
their hosts now and avoid an upgrade churn when confinement ships.
Verification: `uname -r` on the host.

| Distro | Default kernel | Works? |
|---|---|---|
| Ubuntu 22.04 LTS | 5.15+ | Yes |
| Ubuntu 24.04 LTS | 6.8+ | Yes |
| Amazon Linux 2023 | 6.1+ | Yes |
| Debian 12 | 6.1+ | Yes |
| RHEL 9 | 5.14+ | Yes (5.14 includes Landlock backport) |
| Debian 11 | 5.10 | No (upgrade or use backports kernel) |
| Ubuntu 20.04 LTS | 5.4 | No (upgrade to 22.04 LTS) |
| Amazon Linux 2 | 5.10 | No (migrate to AL2023) |

Distro kernel versions last verified 2026-04-27.

---

## Filesystem choice for the sandbox workspace root

cairn-rs stores per-session sandbox state under
`$TMPDIR/cairn-workspace-sandboxes` by default, overridable via the
`CAIRN_SANDBOX_BASE_DIR` env var. When an attempt ends, cairn takes a
snapshot of the agent's write delta. The snapshot cost depends on the
filesystem hosting that directory:

| Filesystem | Snapshot cost | Recommended |
|---|---|---|
| **btrfs** | O(inodes) — ~20ms regardless of size | Yes (fast path) |
| **XFS with reflink=1** | O(inodes) — ~20ms regardless of size | Yes (fast path, default on RHEL 8+) |
| **bcachefs** | O(inodes) | Yes (kernel 6.7+, new on most distros) |
| **ext4** | O(bytes) — ~100ms per 100MB of delta | Works, but slower |
| **tmpfs** | O(bytes), memory-backed | Not recommended (state lost on restart) |

### How to provision a reflink-capable EBS volume on AWS

The default Amazon Linux 2023 AMI uses ext4 for the root volume. For
production deployments you should attach a separate EBS volume formatted
as btrfs or XFS-with-reflink and mount it at `/var/lib/cairn-workspaces`,
then set `CAIRN_SANDBOX_BASE_DIR=/var/lib/cairn-workspaces`.

Attach a 100GB gp3 EBS volume to the instance. Most modern EC2 instance
types (e.g. `m8g`, `m7i`, `c7`, `r7`) expose EBS as NVMe devices under
`/dev/nvme*n1`, while older Xen-based generations use `/dev/xvd*`.
Identify the new volume with:

```bash
lsblk
# or, for NVMe instances:
sudo nvme list
```

Replace `$DEV` below with the block device you identified
(e.g. `/dev/nvme1n1` or `/dev/xvdf`), then:

```bash
sudo mkfs.btrfs "$DEV"
sudo mkdir -p /var/lib/cairn-workspaces
sudo mount "$DEV" /var/lib/cairn-workspaces
echo "$DEV /var/lib/cairn-workspaces btrfs defaults 0 0" | sudo tee -a /etc/fstab
```

Or for XFS with reflink:

```bash
sudo mkfs.xfs -m reflink=1 "$DEV"
sudo mkdir -p /var/lib/cairn-workspaces
sudo mount "$DEV" /var/lib/cairn-workspaces
echo "$DEV /var/lib/cairn-workspaces xfs defaults 0 0" | sudo tee -a /etc/fstab
```

On Amazon Linux 2023, install the tooling with `sudo dnf install btrfs-progs xfsprogs` if it is not already present.

> **Use UUIDs in `/etc/fstab` for production.** Block-device names can
> change across reboots, especially on NVMe. Prefer
> `UUID=<uuid> /var/lib/cairn-workspaces btrfs defaults 0 0`; get the
> UUID from `sudo blkid "$DEV"`.

### What if I stay on ext4?

When reflink is unavailable, cairn is expected to detect this at runtime
and fall back to a byte-copy snapshot. Correctness is preserved; the
snapshot is just slower. For typical agent workloads (<100MB upper-layer
churn per attempt) the difference is imperceptible. For heavy-build
workloads (multi-GB diffs) the fallback can add seconds per
attempt-termination.

The `WorkspaceBackendDegraded` event type is defined in `cairn-domain`
for this signal. Wiring the emission into the workspace provisioner is
part of the same F65 PR-4 sandbox work and not yet live in `main`.

---

## Environment variables

| Variable | Default | Description |
|---|---|---|
| `CAIRN_ADMIN_TOKEN` | random (local) / **required** (team) | Bearer token for operator API auth. Set to a random 32-byte hex string in production. |
| `CAIRN_PORT` | `3000` | HTTP listen port (also settable with `--port`; CLI flag wins). |
| `CAIRN_DB` | in-memory | Storage backend DSN — `memory`, `postgres://…`, `postgresql://…`, or a SQLite path. Also settable with `--db`; CLI flag wins. |
| `CAIRN_MODE` | `local` | Deployment mode: `local` or `team` (alias: `self-hosted`). Also settable with `--mode`; CLI flag wins. |
| `CAIRN_FABRIC_HOST` | `localhost` (bare binary) / `valkey` (compose) | Valkey hostname FlowFabric connects to for lease / lifecycle / eligibility state. |
| `CAIRN_FABRIC_PORT` | `6379` | Valkey port. |
| `CAIRN_FABRIC_WAITPOINT_HMAC_SECRET` | **required** when Fabric is enabled | 32-byte hex secret seeded into every FlowFabric execution partition. Boot fails loud when unset. Rotate at runtime via `POST /v1/admin/rotate-waitpoint-hmac`. See [SECURITY.md](../SECURITY.md). |
| `CAIRN_FABRIC_INSTANCE_ID` | auto-UUID persisted to `/tmp` | Distinguishes this cairn-app process from others sharing the same Valkey. See [operations/cross-instance-isolation.md](./operations/cross-instance-isolation.md). |
| `CAIRN_BACKFILL_INSTANCE_TAG` | unset | When `1`, runs a one-shot boot-time backfill that stamps `cairn.instance_id` onto pre-existing exec-tag hashes that lack it. Only needed for in-place binary swaps with in-flight runs that predate the isolation filter. |

> **Note:** CLI flags always take precedence over the corresponding env var, so you can safely override a container-wide `CAIRN_MODE=team` on a per-invocation basis with `--mode local`. `DATABASE_URL` is honored as a fallback for `CAIRN_DB` when neither is set (the Postgres convention).

### CLI flags reference

```
cairn-app [flags]

  --mode   local|team    Deployment mode (default: local)
  --addr   <ip>          Bind address (default: 127.0.0.1; use 0.0.0.0 for Docker)
  --port   <port>        HTTP listen port (default: 3000)
  --db     <dsn>         Storage backend:
                           postgres://user:pass@host:5432/db  — PostgreSQL
                           /path/to/data.db                   — SQLite
                           (omit for in-memory, local dev only)
  --tls-cert <path>      Path to TLS certificate file (PEM)
  --tls-key  <path>      Path to TLS private key file (PEM)
```

---

## Health check

```
GET /health
```

Returns `200 OK` with a JSON `HealthReport` (`status`: `"healthy"` | `"degraded"`, plus `version`, `uptime_secs`, `store_ok`, per-component `checks`). Returns `503` when the service is unhealthy. No authentication required.  
Safe to use as a load-balancer health probe and liveness check.

```bash
curl -sf http://localhost:3000/health
```

---

## TLS setup

Provide PEM-encoded certificate and key files:

```bash
cairn-app \
  --mode team \
  --addr 0.0.0.0 \
  --port 443 \
  --tls-cert /etc/cairn/tls/cert.pem \
  --tls-key  /etc/cairn/tls/key.pem \
  --db "postgres://cairn:password@localhost:5432/cairn"
```

With Docker:

```bash
docker run -d \
  -p 443:443 \
  -v /etc/cairn/tls:/tls:ro \
  -e CAIRN_ADMIN_TOKEN="..." \
  cairn-rs \
  --mode team \
  --addr 0.0.0.0 \
  --port 443 \
  --tls-cert /tls/cert.pem \
  --tls-key  /tls/key.pem \
  --db "postgres://cairn:password@db.internal:5432/cairn"
```

> **TLS is required in `--mode team`.** cairn-app will refuse to start in team mode without a TLS certificate.

Let's Encrypt with Certbot:

```bash
certbot certonly --standalone -d cairn.example.com
# Certificates written to /etc/letsencrypt/live/cairn.example.com/
--tls-cert /etc/letsencrypt/live/cairn.example.com/fullchain.pem
--tls-key  /etc/letsencrypt/live/cairn.example.com/privkey.pem
```

---

## Systemd service (non-Docker)

Install the binary:

```bash
cp target/release/cairn-app /usr/local/bin/cairn-app
chmod 755 /usr/local/bin/cairn-app
```

Create `/etc/systemd/system/cairn.service`:

```ini
[Unit]
Description=cairn control-plane
Documentation=https://github.com/avifenesh/cairn-rs
After=network.target postgresql.service
Requires=postgresql.service

[Service]
Type=simple
User=cairn
Group=cairn

# Admin token — store in /etc/cairn/env or use systemd-creds
EnvironmentFile=/etc/cairn/env

ExecStart=/usr/local/bin/cairn-app \
  --mode team \
  --addr 0.0.0.0 \
  --port 3000 \
  --tls-cert /etc/cairn/tls/cert.pem \
  --tls-key  /etc/cairn/tls/key.pem \
  --db "postgres://cairn:password@localhost:5432/cairn"

Restart=on-failure
RestartSec=5s
TimeoutStopSec=30s

# Harden the process.
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/lib/cairn

[Install]
WantedBy=multi-user.target
```

`/etc/cairn/env`:

```bash
CAIRN_ADMIN_TOKEN=your-32-byte-hex-token-here
```

Enable and start:

```bash
# Create the cairn system user
useradd --system --no-create-home --shell /usr/sbin/nologin cairn

# Create data directory
install -d -o cairn -g cairn /var/lib/cairn

sudo systemctl daemon-reload
sudo systemctl enable cairn
sudo systemctl start cairn
sudo systemctl status cairn

# Tail logs
journalctl -u cairn -f
```

---

## Upgrading

```bash
# Docker Compose
docker compose pull
docker compose up -d --build

# Systemd
systemctl stop cairn
cp target/release/cairn-app /usr/local/bin/cairn-app
systemctl start cairn
```

Migrations run automatically on startup.
