# CVMFS Infrastructure Modernisation Analysis

## 1. Current Architecture

### 1.1 The canonical deployment (WLCG / HEP grid)

```
Publisher workstation
        │  cvmfs_server publish / gateway lease
        ▼
┌───────────────┐
│  Stratum 0    │  Single write-authoritative origin.
│  Apache/Nginx │  Serves HTTP; not directly exposed to clients.
│  local disk   │  ~1–4 TB NVMe or Ceph RBD.
│  or S3 bucket │
└──────┬────────┘
       │  cvmfs_server snapshot (pull, every 5–15 min or webhook)
       │
┌──────▼────────────────────────────────────────────────────────┐
│  Stratum 1 mirrors  (10–50 globally for CMS/ATLAS/LHCb repos) │
│  Apache + mod_wsgi  serving static content-addressed files     │
│  Each: ~10–40 TB disk, 10 Gbps uplink                         │
│  Sites: CERN, BNL, IN2P3, GridKA, NIKHEF, RAL, TRIUMF, …    │
└──────┬────────────────────────────────────────────────────────┘
       │  HTTP (port 8000)
       │
┌──────▼──────────────────────────────────────────────────────────┐
│  Site Squid proxy  (1–3 per grid site, ~700 sites worldwide)    │
│  Squid 3/4/5  with disk cache (200 GB – 2 TB)                  │
│  Config: cache_peer to Stratum 1, hierarchy_stoplist, acl      │
│  WPAD / PAC file or static CVMFS_HTTP_PROXY on each worker     │
└──────┬──────────────────────────────────────────────────────────┘
       │  HTTP (port 3128)
       │
┌──────▼─────────────────────────────────────────────────────────┐
│  Worker nodes  (batch farm: 10 – 100,000 per site)             │
│  cvmfs2 FUSE client  /cvmfs/<repo>                             │
│  Local page cache (kernel + CVMFS client cache, ~20 GB)        │
└────────────────────────────────────────────────────────────────┘
```

### 1.2 Client request lifecycle

1. FUSE open triggers catalog lookup → client checks its local SQLite cache.
2. On miss: HTTP GET to Squid (CVMFS_HTTP_PROXY).
3. Squid checks its disk cache. On hit: served from disk (~0.1 ms local LAN).
4. On Squid miss: Squid fetches from Stratum 1 (GeoAPI-sorted by client IP).
5. Stratum 1 serves from local disk or fetches from Stratum 0 if not yet
   replicated.
6. Chunk data flows back to client; client assembles file via FUSE and
   populates kernel page cache.

### 1.3 Consistency and TTL

- Root catalog TTL is embedded in the catalog itself (typically 4–240 min).
- Clients poll for new root catalog hash every TTL seconds.
- Stratum 1 replication lag adds to effective staleness (minutes to ~2 h in
  busy deployments without webhook notification).
- No push mechanism to clients; all consistency is pull-based with TTL expiry.

### 1.4 Key pain points

| Area | Problem |
|---|---|
| **Squid ops burden** | Each of ~700 sites runs 1–3 Squid instances. Requires local sysadmin expertise: config, disk management, cache tuning, security patches. |
| **Squid age** | Squid 3/4 is a 25-year-old C codebase. Limited observability, TLS termination is awkward, no native HTTP/2. |
| **Stratum 1 lag** | Pull-based replication adds 5–120 min staleness. Clients hold stale catalog views during the window. |
| **Cold-start thundering herd** | After a publish, 100s of workers simultaneously miss the Squid cold cache, hammering Stratum 1. |
| **GeoAPI coupling** | Client Stratum 1 selection depends on a custom GeoAPI call. Single point of coordination, not topology-aware within a region. |
| **WPAD/PAC fragility** | Sites configure CVMFS_HTTP_PROXY manually or via WPAD. Misconfigured proxies silently bypass caching. |
| **No negative caching** | Every lookup for a missing path goes all the way to the server; no bloom filter or negative-cache in the proxy tier. |
| **Monitoring gaps** | Squid hit rates and S1 replication state are monitored inconsistently across sites; no unified observability plane. |

---

## 2. Why a Commercial CDN Is Not the Answer

The properties of CVMFS content — entirely content-addressed, immutable data
objects with a small set of short-TTL mutable manifests — make it structurally
ideal for CDN-style distribution.  A commercial CDN (Cloudflare, Fastly,
CloudFront) would eliminate the Stratum 1 and Squid tiers in one step.  The
argument is attractive on paper but fails on four grounds that are specific to
HEP grid operations.

### 2.1 Research-network policy and routing

WLCG, OSG, and EGI traffic flows predominantly through dedicated research
networks — GÉANT (Europe), ESnet (US), NorduNet, KISTI (Korea), RNP (Brazil)
and their national affiliates.  These networks have bilateral peering agreements
with each other and with CERN, BNL, IN2P3 and other Tier-1 centres that
guarantee high bandwidth at zero marginal cost.  Commercial CDN PoPs sit outside
this peering fabric: traffic routed through a commercial PoP must traverse the
public internet at some point, losing the bandwidth guarantees and potentially
incurring latency penalties on cross-continent paths that the research network
peering was specifically designed to avoid.  Several national networks explicitly
prohibit commercial CDN intermediation of traffic classified as "research data"
in their acceptable-use policies.

### 2.2 Cost at HEP scale

The top five HEP software repositories collectively serve O(10⁸) file-opens per
day across ~100,000 active worker cores.  Even with generous object-level cache
hit rates (>98% for data chunks, ~80% for catalogs at a warm site proxy), the
residual Stratum 1 → client traffic is in the tens of terabytes per day.
Commercial CDN egress pricing — typically $0.04–$0.09/GB after negotiated
discounts — would translate to $200,000–$500,000/year for the WLCG software
corpus alone.  Research-programme pricing exists (Cloudflare for
Nonprofits, Internet Archive partnerships) but is not guaranteed, subject to
terms changes, and does not cover the full WLCG traffic volume.

### 2.3 Data governance and sovereignty

Collaboration software tarballs and conditions databases may contain
pre-publication components subject to experiment-internal data-handling
agreements.  Routing this content through commercial infrastructure whose data
retention, access-logging, and jurisdiction policies are outside the
collaboration's control is incompatible with those agreements.  CVMFS
cryptographic signing provides object integrity but not confidentiality; once
content transits a third-party PoP, the collaboration loses audit-trail control.

### 2.4 Protocol opacity

Commercial CDNs treat all traffic as opaque HTTP.  They cannot participate in
CVMFS-specific mechanisms: the cvmfs-bits push protocol for zero-latency cache
seeding, bloom-filter negotiation for object presence, catalog-tree-aware
pre-warming, or the cryptographic whitelist verification that CVMFS clients
perform on catalog files.  A commercial CDN would serve objects correctly but
would be blind to the CVMFS consistency model, making the push-based
replication improvements developed in this branch inapplicable.

---

## 3. HepCDN — A Community-Owned Content Delivery Network Seeded by CVMFS

### 3.1 The core insight: HEP already operates a CDN

The existing CVMFS distribution infrastructure is, structurally, a content
delivery network.  It has PoPs (Stratum 1 mirrors at Tier-1 and Tier-2 sites),
edge caches (site Squid proxies), a routing layer (GeoAPI), and an origin
(Stratum 0).  What it lacks is:

- a **unified coordination plane** (topology registry, health state, routing
  policy) operated as a service rather than embedded in static config files;
- a **push seeding protocol** to fill caches before clients request content
  (current: pull-on-demand with TTL-gated revalidation);
- **modern cache software** at the edge (Squid → Varnish/Nginx);
- **unified observability** across all tiers.

HepCDN is the proposal to add exactly these four missing pieces to the existing
hardware, using CVMFS Stratum 0/1 as the seeding layer.  No new PoP hardware
is needed for an initial deployment; the upgrade path is purely software.

### 3.2 Proposed architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  CVMFS Stratum 0  +  cvmfs-bits distributor  (Seed tier)            │
│                                                                      │
│  On publish:                                                         │
│  1. New objects pushed to all Tier-1 nodes via cvmfs-bits protocol  │
│  2. Bloom filter snapshot broadcast to Tier-2 edge caches           │
│  3. Root catalog hash announced to HepCDN coordination service      │
└───────────────────────┬──────────────────────────────────────────────┘
                        │  push (cvmfs-bits)  +  announce
                        │
┌───────────────────────▼──────────────────────────────────────────────┐
│  HepCDN Tier-1  (Regional nodes — existing Stratum 1 hardware)       │
│                                                                      │
│  Upgraded software stack per node:                                   │
│  • Varnish or Nginx (replaces Apache) with CVMFS-tuned cache rules  │
│  • cvmfs-bits receiver: accepts pushed objects, writes to cache      │
│  • Bloom filter service: answers "do you have <hash>?" queries       │
│  • Prometheus exporter: hit rate, fill %, replication lag, uptime   │
│  • Registers with HepCDN coordination service at startup            │
│                                                                      │
│  Nodes: CERN, BNL, IN2P3, GridKA, NIKHEF, RAL, TRIUMF, KISTI, …  │
└───────────────────────┬──────────────────────────────────────────────┘
                        │  HTTP/2 or HTTP/3
                        │  (routed by coordination service)
┌───────────────────────▼──────────────────────────────────────────────┐
│  HepCDN Tier-2  (Site edge — existing Squid hardware, new software)  │
│                                                                      │
│  Varnish replacing Squid:                                            │
│  • Upstream selected from coordination service (not static config)   │
│  • On bloom-filter hit: pre-warm cache from Tier-1 push signal       │
│  • Negative caching: 404 responses cached 30 s                      │
│  • Prometheus metrics → site and central monitoring                  │
└───────────────────────┬──────────────────────────────────────────────┘
                        │  HTTP (LAN, <0.5 ms)
                        ▼
                  Worker nodes
                  CVMFS client: CVMFS_SERVER_URL points to
                  coordination service (resolves to nearest Tier-1)
                  CVMFS_HTTP_PROXY points to site Tier-2
```

### 3.3 HepCDN coordination service

A lightweight Go service (one or two active replicas, stateless query path)
replacing the existing GeoAPI.  It exposes:

```
GET /route?client=<IP>&repo=<name>
→  { "tier1": ["https://cvmfs-s1.cern.ch", "https://cvmfs-s1.bnl.gov"],
     "bloom_endpoint": "https://cvmfs-s1.cern.ch/bloom/<repo>" }

GET /health
→  { "tier1_nodes": 14, "healthy": 13, "seeding_lag_ms": 420 }

POST /announce   (called by cvmfs-bits after publish)
→  triggers Tier-2 edge pre-warm signal broadcast
```

Routing logic: map client IP → AS number → research-network topology graph
(derived from GÉANT/ESnet BGP data, updated weekly) → nearest healthy Tier-1
node with confirmed object availability (bloom filter).  This is significantly
more accurate than the IP-geolocation approach used by GeoAPI today, which
misroutes clients behind VPNs and multi-homed sites.

### 3.4 CVMFS as the seeding layer

CVMFS Stratum 0 is uniquely positioned as the seeding source because it has:

**Complete object inventory.** The reflog and catalog tree enumerate every
object hash in the repository.  The cvmfs-bits distributor can generate a
precise list of new objects after each publish — no directory scan, no
heuristic, no over-fetching.

**Cryptographic integrity.** Every object is content-addressed by SHA-256 or
BLAKE2b.  A Tier-1 node that receives a pushed object can verify it
independently without trusting the push source.  The existing CVMFS whitelist
and signature chain extends naturally to HepCDN nodes.

**Publish-time knowledge.** The distributor runs immediately after the ingestion
pipeline and before the gateway lease is committed.  Tier-1 caches are
pre-filled with the new objects before the new root catalog hash is visible to
any client, eliminating the cold-start thundering herd by construction.

**Differential pushes.** The bloom filter maintained per publish allows the
distributor to push only the delta (objects in the new catalog tree that are
absent from a node's current bloom filter), keeping seeding bandwidth
proportional to the publish size, not the total repository size.

### 3.5 What changes at each tier

| Component | Today | HepCDN |
|---|---|---|
| Stratum 0 | Publishes, serves origin | Publishes + runs cvmfs-bits distributor; announces to coordination service |
| Stratum 1 | Periodic pull, Apache serve | Receives pushed objects, Varnish serve, exports metrics, registers with coordination service |
| Site proxy | Squid, static upstream list | Varnish, upstream from coordination service, pre-warms on push signal |
| Client config | `CVMFS_SERVER_URL` = static list; `CVMFS_HTTP_PROXY` = static list | `CVMFS_SERVER_URL` = coordination service URL; `CVMFS_HTTP_PROXY` = site Varnish |
| Routing | GeoAPI (IP geolocation) | Coordination service (AS-topology + bloom-filter-confirmed availability) |
| Replication | Pull every 5–15 min | Push on publish (<5 s lag) |
| Observability | Per-site, inconsistent | Unified Prometheus + Grafana dashboard |

### 3.6 Comparison with commercial CDN

| Criterion | Commercial CDN | HepCDN |
|---|---|---|
| Research-network routing | No — public internet egress | Yes — stays on GÉANT/ESnet |
| PoPs at major grid sites | No | Yes (existing Stratum 1) |
| Marginal cost per GB | $0.04–$0.09 | ~$0 (existing hardware) |
| CVMFS push seeding | Not possible | Native (cvmfs-bits) |
| Bloom-filter presence queries | Not possible | Native |
| Catalog-aware pre-warming | Not possible | Native |
| Data governance | Third-party jurisdiction | Full collaboration control |
| Cryptographic audit trail | Opaque to CDN | End-to-end verifiable |
| New PoP deployment | Self-service (minutes) | New Stratum 1 + registration |
| Cold-start thundering herd | Absorbed at PoP (random) | Eliminated by push pre-fill |

The fundamental asymmetry is that a commercial CDN is a generic HTTP cache that
happens to work adequately for content-addressed objects, while HepCDN is a
distribution system that understands CVMFS semantics end-to-end.  The latter
enables capabilities — push seeding, differential bloom-filter updates,
topology-aware routing within the research-network fabric — that are
structurally impossible with an opaque third-party intermediary.

---

## 4. Other Improvements (Independent of HepCDN)

### 4.1 Varnish or Nginx as a drop-in Squid replacement  ★ near-term

Replace site Squid instances with Varnish Cache or Nginx proxy_cache.  This
improvement is a prerequisite for HepCDN Tier-2 (the pre-warm signalling and
Prometheus integration require Varnish or Nginx) but is also valuable
standalone, independent of the coordination layer.

#### 4.1.1 Architecture comparison

```
CURRENT — Squid                         PROPOSED — Varnish / Nginx
═══════════════════════════════════     ══════════════════════════════════════
Stratum 1                               Stratum 1 / HepCDN Tier-1
┌─────────────────────────────┐         ┌──────────────────────────────────────┐
│ Apache HTTP                 │         │ Varnish + cvmfs-bits receiver        │
│ pull replication            │         │ Prometheus exporter                  │
│ no metrics                  │         │ HTTP/2 listen                        │
└──────────┬──────────────────┘         └───────────┬──────────────────────────┘
           │ HTTP/1.1 (new TCP per req)             │ HTTP/2 multiplexed
           ▼                                        ▼
┌──────────────────────────┐            ┌──────────────────────────────────────┐
│  Squid proxy (site)      │            │  Varnish / Nginx (site Tier-2)       │
│  ✗ No Prometheus         │            │  ✓ Prometheus /metrics               │
│  ✗ No HTTP/2 upstream    │            │  ✓ HTTP/2 upstream to S1             │
│  ✗ Coarse TTL control    │            │  ✓ Per-object TTL (VCL/config)       │
│  ✗ Opaque disk cache     │            │  ✓ Pre-warm on push signal           │
└──────────┬───────────────┘            └───────────┬──────────────────────────┘
           │                                        │
           ▼                                        ▼
  CVMFS clients (workers)                 CVMFS clients (workers)
  CVMFS_HTTP_PROXY=squid:3128             CVMFS_HTTP_PROXY=varnish:80
                                          (drop-in replacement, no client change)

  MISS path: Squid → S1 (HTTP/1.1)       MISS path: Varnish → S1 (HTTP/2)
                                          PUSH path: S1 → Varnish pre-warm ←──
                                                     (eliminates cold-start herd)
```

#### 4.1.2 Feature comparison

| Feature | Squid 3/4/5 | Varnish Cache | Nginx proxy_cache |
|---|---|---|---|
| Cache HIT latency | 3–10 ms (disk I/O) | **0.5–2 ms (RAM)** | 1–3 ms (sendfile) |
| Upstream protocol | HTTP/1.1 only | HTTP/2 (`vmod_http2`) | HTTP/2 (`http2`) |
| Per-object TTL rules | `refresh_pattern` (coarse) | **VCL: exact per-URL** | `proxy_cache_valid` (per-code) |
| Prometheus metrics | None (cachemgr.cgi only) | **varnish-exporter** | nginx-prometheus-exporter |
| Push pre-warm support | No | **Yes (VCL PURGE + BAN)** | Yes (proxy_cache_purge) |
| Stale-while-revalidate | Limited | **Grace mode** | `proxy_cache_use_stale` |
| Negative caching (404) | Requires `negative_ttl` | **VCL explicit** | `proxy_cache_valid 404` |
| Config management | Per-site squid.conf | **Central VCL template** | Central nginx.conf |
| Active security patches | Slow (C codebase, OSS) | Active (2–4 wk CVE lag) | Active (nginx core team) |
| Memory footprint | ~100–300 MB | ~50–150 MB | ~30–100 MB |

#### 4.1.3 Varnish VCL caching rules for CVMFS

The key insight is that CVMFS objects are content-addressed: a hash-named
data file never changes, so it can be cached indefinitely, while catalog and
manifest files must respect the 60-second TTL.  Squid cannot express this
distinction cleanly; Varnish VCL makes it explicit:

```vcl
sub vcl_backend_response {
    # Content-addressed data objects: hash is in the URL → cache forever.
    if (bereq.url ~ "^/data/[0-9a-f]{2}/[0-9a-f]{38}") {
        set beresp.ttl    = 365d;
        set beresp.grace  = 30d;   # serve stale while revalidating
        unset beresp.http.Set-Cookie;
        return (deliver);
    }
    # Catalog and manifest files: respect the 60-second polling interval.
    if (bereq.url ~ "\.(cvmfspublished|cvmfswhitelist|cvmfschecksum)$"
        || bereq.url ~ "^/.cvmfs") {
        set beresp.ttl   = 60s;
        set beresp.grace = 10s;
        return (deliver);
    }
    # Default: 5-minute TTL for anything else.
    set beresp.ttl = 5m;
}
```

The equivalent Nginx stanza is more compact but less expressive:

```nginx
proxy_cache_path /var/cache/nginx/cvmfs levels=2:2
    keys_zone=cvmfs:64m max_size=2t inactive=365d;

location ~ ^/data/[0-9a-f]{2}/[0-9a-f]{38} {
    proxy_cache            cvmfs;
    proxy_cache_valid      200 365d;
    proxy_cache_use_stale  error timeout updating;
    proxy_pass             http://stratum1_upstream;
}
location ~ \.(cvmfspublished|cvmfschecksum|cvmfswhitelist)$ {
    proxy_cache            cvmfs;
    proxy_cache_valid      200 60s;
    proxy_pass             http://stratum1_upstream;
}
```

#### 4.1.4 Quantified performance and savings estimates

**Reference site:** 2,000 grid workers, 200 GB Squid disk cache, average
cache hit rate 85%, publish frequency 4×/day, catalog check period 5 min.

**Latency improvement (cache HIT path):**

| Path | Squid | Varnish | Saving |
|---|---|---|---|
| Cache hit (data object) | 5 ms (avg disk seek + TCP) | 1 ms (RAM, kernel sendfile) | −4 ms |
| Catalog check (60 s cycle, 2,000 workers) | 5 ms × 2,000 = 10 CPU-s/min | 1 ms × 2,000 = 2 CPU-s/min | −80% worker idle wait |
| Catalog miss (fetch from S1) | 200 ms (HTTP/1.1, new TCP) | 160 ms (HTTP/2, reuse) | −20% miss latency |

**Cold-start thundering herd (post-publish):**

With Squid, 2,000 workers simultaneously poll for the new catalog, all miss,
and all open connections to the same Stratum 1 simultaneously — a burst of
2,000 HTTP requests within a 5-second window.  With Varnish pre-warmed via a
cvmfs-bits push signal, the new catalog is in RAM before any worker requests
it; the burst drops to zero upstream requests.

| Scenario | S1 upstream requests (5-min window post-publish) |
|---|---|
| Squid (cold cache) | ~2,000 connections in < 10 s |
| Varnish + push pre-warm | 0–1 (only the pre-warm fetch) |

**Ops savings across the WLCG grid (~700 sites):**

| Activity | Squid | Varnish/Nginx | Saving |
|---|---|---|---|
| Initial config per site | 4 h manual | 30 min (Puppet template) | −87% |
| Annual maintenance per site | 8 h (patches, tuning, restarts) | 1 h (OS package update) | −87% |
| Emergency response (security CVE) | 2–5 days, all sites manually | 1 PR → Puppet → all sites in 2 h | −95% |
| Total grid-wide ops (700 sites) | ~700 × 8 h = 5,600 h/yr | ~700 × 1 h = 700 h/yr | **−4,900 h/yr** |

At a representative sysadmin cost of €80/h, that is a saving of approximately
**€390,000/year** across the grid — without any hardware procurement.

**Stratum 1 upstream load:**

HTTP/2 multiplexing allows Varnish to reuse a single TCP connection for all
concurrent upstream fetches to a given S1.  For a site with 2,000 workers
and 15% miss rate, the number of simultaneous S1 connections drops from
~300 (Squid, one TCP per request) to ~10 (Varnish, H2 streams).

**Nginx advantages over Varnish (simpler sites):**

- Already installed at many sites (reverse proxy for other services) — zero
  new package dependencies.
- `proxy_cache_purge` module handles push pre-warm via a simple HTTP PURGE
  request from the cvmfs-bits signal handler.
- Lower memory footprint; suitable for sites with < 64 GB RAM.
- Slightly lower peak throughput than Varnish for very hot objects (Varnish
  worker threads vs Nginx event loop), but negligible at typical HEP site scale.

**Migration path:** same hardware, same port (3128 → 80 or keep 3128 with
Varnish listening on that port), `CVMFS_HTTP_PROXY` unchanged if a
site-local DNS alias is used.  1–2 days per site; fully templated centrally
via Puppet/Ansible.  Reference VCL for HepCDN Tier-2 is in §6.

---

### 4.2 Kubernetes-native Stratum 1  ★ medium-term ops simplification

Replace manually managed Stratum 1 VMs with Kubernetes workloads (Deployment +
HPA + PVC).

```yaml
deployment:
  image: cvmfs/stratum1-hepcdn:latest   # adds cvmfs-bits receiver + exporter
  replicas: 2
  autoscaling:
    minReplicas: 1
    maxReplicas: 8
    targetCPUUtilizationPercentage: 60
storage:
  storageClass: ceph-rbd
  size: 20Ti
```

Auto-scaling handles post-publish bursts; rolling updates for cvmfs-bits
receiver upgrades require zero downtime; health checks restart unhealthy
replicas automatically.  Declarative config managed in Git and reviewed via PR.

---

### 4.3 HTTP/3 (QUIC) transport for client downloads  ★ medium-term performance

CVMFS clients currently use HTTP/1.1 via libcurl over TCP.  QUIC (RFC 9000)
provides:

- **0-RTT connection resumption**: catalog checks from returning workers take
  one fewer round-trip (~10–50 ms saving on high-latency WAN links).
- **Multiplexed streams without HOL blocking**: parallel chunk fetches over a
  single QUIC connection avoid TCP head-of-line blocking.
- **Connection migration**: dual-NIC or VPN transitions don't break downloads.

**Implementation path:** libcurl ≥ 7.88.0 supports QUIC; Nginx ≥ 1.25.0
serves HTTP/3.  Adding `CURLOPT_HTTP_VERSION = CURL_HTTP_VERSION_3` with
fallback to HTTP/2 in the CVMFS download manager requires ~50 lines of change.

**Estimated gain (500 km WAN, 5 ms RTT):** ~40 ms per catalog check, ~15%
throughput improvement for many-small-file workloads.

---

### 4.4 Push-based cache seeding (cvmfs-bits)  ★ eliminates replication lag

Implemented in this branch.  After publish, Stratum 0 pushes new objects
directly to all registered HepCDN Tier-1 and Tier-2 nodes via the cvmfs-bits
distributor before the gateway lease is committed.  In HepCDN context:

- The distributor queries each Tier-1 node's bloom filter to identify which
  objects are missing, then pushes only the delta.
- Tier-1 nodes relay push signals to downstream Tier-2 caches, which
  pre-warm the objects most likely to be requested at job startup.
- The coordination service is notified of the new root catalog hash only after
  all Tier-1 nodes confirm object receipt, guaranteeing that clients routed to
  a Tier-1 node by the coordination service will always find the objects they
  request.

Result: replication lag drops from 5–120 min to < 5 s; cold-start thundering
herd is eliminated.

---

### 4.5 eBPF-accelerated in-kernel object cache  ★ long-term / experimental

An XDP/eBPF program on a Tier-1 or Tier-2 host intercepts CVMFS HTTP GETs at
the NIC, looks up the content hash in a shared BPF map, and returns cached
bytes directly from a pinned memory region — bypassing the TCP stack entirely.

**Realistic gains:** for hot objects (root catalog polled every ~5 min by
10,000 workers): 0.1 ms → < 0.01 ms at the edge node.

**Maturity:** proof-of-concept only.  Requires kernel ≥ 5.15 and privileged
container access.  Not recommended for near-term deployment.

---

### 4.6 OCI Distribution as transport layer  ★ speculative

CVMFS objects are structurally identical to OCI image layers: content-addressed
blobs referenced by a manifest.  An OCI Distribution v1.1 registry (Zot,
Harbor) could serve as a Tier-1 node, with registry replication replacing
cvmfs_server snapshot.  Suitable as a long-term research direction; the CVMFS
client would need to speak the OCI blob API, which is a non-trivial change.

---

## 5. Recommended Roadmap

```
Now  ──────────────────────────────────────────────────── +3 years

 Phase 1 (0–6 months): Prerequisite infrastructure
 ├─ Replace site Squid with Varnish (central Puppet/Ansible template)
 ├─ Deploy cvmfs-bits distributor at Stratum 0 (done in this branch)
 ├─ Enable webhook-triggered S1 replication (eliminates polling lag)
 └─ Instrument Stratum 1 nodes with Prometheus exporters

 Phase 2 (6–18 months): HepCDN v1 — coordination + seeding
 ├─ Deploy HepCDN coordination service (routing API, health registry)
 ├─ Upgrade 3–5 pilot Stratum 1 nodes to HepCDN Tier-1 software stack
 │   (cvmfs-bits receiver, bloom filter service, coordination registration)
 ├─ Wire Varnish Tier-2 nodes to receive push pre-warm signals
 ├─ Update CVMFS client documentation: coordination service URL replaces
 │   static CVMFS_SERVER_URL list
 └─ Unified Prometheus/Grafana dashboard across all tiers

 Phase 3 (18–36 months): HepCDN full rollout
 ├─ All Stratum 1 nodes registered as HepCDN Tier-1
 ├─ K8s-native Tier-1 deployment at 3+ sites (pilot)
 ├─ AS-topology routing replaces GeoAPI at all registered sites
 ├─ HTTP/3 support in CVMFS client (libcurl QUIC, opt-in flag)
 └─ Differential bloom-filter seeding for large repos (CMS, ATLAS)

 Phase 4 (36+ months): Research and extension
 ├─ New grid sites join HepCDN by registering a Varnish node
 │   (no Stratum 1 hardware required for pure Tier-2 membership)
 ├─ OCI Distribution as optional Tier-1 backend (research)
 └─ eBPF catalog cache at highest-traffic Tier-1 nodes (research)
```

### Priority matrix

| Option | Ops saving | Perf gain | Disruption | Effort | Phase |
|---|---|---|---|---|---|
| Varnish/Nginx replaces Squid | High | Medium | Low | Low | **1 — now** |
| cvmfs-bits push seeding | High | Very high | Low | Low | **1 — now** |
| S1 Prometheus instrumentation | Medium | — | Very low | Low | **1 — now** |
| HepCDN coordination service | Very high | High | Medium | Medium | **2** |
| Tier-1 software upgrade (pilot) | High | High | Low | Medium | **2** |
| Unified observability dashboard | Medium | — | Very low | Low | **2** |
| Full HepCDN Tier-1 rollout | Very high | Very high | Medium | Medium | **3** |
| K8s-native Tier-1 | High | Low | Medium | Medium | **3** |
| HTTP/3 client transport | Low | Medium | Very low | Low | **3** |
| AS-topology routing | Medium | Medium | Low | Medium | **3** |
| OCI Distribution backend | Medium | Low | Very high | Very high | Research |
| eBPF cache | Low | Very high (hot) | High | Very high | Research |

---

## 6. Reference Varnish Configuration for HepCDN Tier-2

Squid is the biggest operational burden and the area where a drop-in replacement
delivers the most value with the least risk.

**Why sites keep Squid today:**
- CVMFS documentation explicitly recommends it.
- Existing Puppet modules at WLCG sites configure it.
- Squid's `cache_peer` hierarchy mirrors how CVMFS proxy lists work.

**Varnish feature parity:**

| Squid feature | Varnish equivalent |
|---|---|
| `cache_dir ufs` | `storage = file` |
| `cache_peer` hierarchy | `bereq.backend` in `vcl_backend_fetch` |
| `acl localnet` | `req.http.X-Forwarded-For` match in VCL |
| `maximum_object_size` | `beresp.http.Content-Length` check in VCL |
| `refresh_pattern` | `beresp.ttl` override in `vcl_backend_response` |
| `log_format` | `varnishncsa -F` with custom format |
| Prometheus metrics | `varnish_exporter` sidecar |

**Reference VCL for a HepCDN Tier-2 node** (upstream list supplied by
coordination service; pre-warm hook called on push signal from Tier-1):

```vcl
vcl 4.1;

# Upstream Tier-1 nodes — in production, populated from HepCDN coordination
# service via a config-management template (Puppet/Ansible/Helm).
backend tier1_a { .host = "cvmfs-s1.cern.ch";    .port = "8000"; }
backend tier1_b { .host = "cvmfs-s1.bnl.gov";    .port = "8000"; }

import directors;

sub vcl_init {
    new vd = directors.round_robin();
    vd.add_backend(tier1_a);
    vd.add_backend(tier1_b);
}

sub vcl_recv {
    set req.backend_hint = vd.backend();
    # Normalise: strip Range headers for small catalog objects to maximise
    # cache key collision and hit rate.
    if (req.url !~ "^/data/") { unset req.http.Range; }
    return(hash);
}

sub vcl_backend_response {
    # Content-addressed data objects are immutable: cache indefinitely.
    if (bereq.url ~ "^/data/[0-9a-f]{2}/[0-9a-f]+$") {
        set beresp.ttl = 365d;
        set beresp.grace = 24h;
        return(deliver);
    }
    # Root catalog and whitelist: short TTL, serve stale while revalidating.
    if (bereq.url ~ "\.cvmfspublished$|\.cvmfswhitelist$") {
        set beresp.ttl = 60s;
        set beresp.grace = 300s;
        return(deliver);
    }
    # Catalog SQLite databases are versioned by hash in path: immutable.
    if (bereq.url ~ "\.cvmfsc(\.gz)?$") {
        set beresp.ttl = 365d;
        return(deliver);
    }
    # Negative caching: suppress thundering-herd retries for absent paths.
    if (beresp.status == 404) {
        set beresp.ttl = 30s;
        return(deliver);
    }
}

sub vcl_deliver {
    # Expose cache status for monitoring and debugging.
    set resp.http.X-Cache        = (obj.hits > 0) ? "HIT" : "MISS";
    set resp.http.X-Cache-Hits   = obj.hits;
    set resp.http.X-Served-By    = server.hostname;
}
```

This config replicates the functionally important parts of a CVMFS Squid setup
in ~45 lines of readable VCL versus ~120 lines of opaque Squid directives, and
adds negative caching, stale-while-revalidate, and structured cache-status
headers as a bonus.
