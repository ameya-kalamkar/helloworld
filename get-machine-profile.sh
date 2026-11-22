#!/usr/bin/env bash
# =============================================================================
# get-machine-profile.sh — read-only hardware / OS / storage profile of this host
# Target: Red Hat Enterprise Linux 8.x.  Version 1.5.  Run as root.
#
# READ-ONLY. NEVER INSTALLS OR CHANGES ANYTHING. It checks every command it may
# use, records which are found / missing (with the RHEL 8 package name), and
# SKIPS any check whose command is missing. The only files written are the
# results below.
#
# Output:  /tmp/machine-profile/<host>/<timestamp>/
#            summary.txt   one-page machine profile           (needs python)
#            report.txt    every raw output, in order
#            deps.txt      dependency table
#            raw/*.txt     one file per check
#          /tmp/machine-profile/<host>/<timestamp>.tar.gz   (archive of the above)
#          Result files are read-only for everyone (444), result dirs 555.
#
# Usage:
#   ./get-machine-profile.sh --check              dependency table only, then exit
#   ./get-machine-profile.sh                      collect the profile (~1-2 min)
#   ./get-machine-profile.sh --summarize DIR      rebuild summary.txt from an existing result DIR
#
# Python is optional and only builds summary.txt (python3.11, python3, or RHEL's
# built-in /usr/libexec/platform-python). Standard library only.
# =============================================================================
set -u

CHECK_ONLY=0; SUMMARIZE_ONLY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --check)     CHECK_ONLY=1; shift ;;
    --summarize) SUMMARIZE_ONLY="${2:-}"; [[ -d "$SUMMARIZE_ONLY" ]] || { echo "--summarize needs an output folder"; exit 1; }; shift 2 ;;
    -h|--help)   sed -n '2,25p' "$0"; exit 0 ;;
    *) echo "Unknown arg: $1 (see --help)"; exit 1 ;;
  esac
done

PROFILE_ROOT="/tmp/machine-profile"   # results always go here

have() { command -v "$1" >/dev/null 2>&1; }
IS_ROOT=0; [[ $(id -u) -eq 0 ]] && IS_ROOT=1

PY=""   # prefer python3.11 (RHEL 8 AppStream), then python3, then RHEL's built-in platform-python
for c in python3.11 python3.12 python3 /usr/libexec/platform-python; do
  if have "$c" || [[ -x "$c" ]]; then PY=$c; break; fi
done

# -----------------------------------------------------------------------------
# Dependencies:  command | RHEL 8 package | root? (Y/N) | mode | used for
# -----------------------------------------------------------------------------
DEPS=(
  "lscpu|util-linux|N|core|CPU model, sockets, cores, caches, flags"
  "lsblk|util-linux|N|core|block devices, scheduler, read-ahead"
  "findmnt|util-linux|N|core|mounts and mount options"
  "swapon|util-linux|N|core|swap devices"
  "free|procps-ng|N|core|memory totals"
  "sysctl|procps-ng|N|core|kernel VM / NUMA settings"
  "df|coreutils|N|core|filesystem and /dev/shm size"
  "timeout|coreutils|N|core|guard against hanging commands"
  "numactl|numactl|N|core|NUMA nodes, memory per node"
  "numastat|numactl|N|core|per-node memory usage"
  "lspci|pciutils|N|core|identify FC HBA cards"
  "systool|sysfsutils|N|core|detailed FC host attributes"
  "modinfo|kmod|N|core|HBA driver versions"
  "systemctl|systemd|N|core|irqbalance / multipathd service state"
  "tuned-adm|tuned|N|core|active tuning profile"
  "ip|iproute|N|core|network interfaces"
  "xfs_info|xfsprogs|N|core|XFS geometry (stripe unit/width)"
  "java|(your JDK)|N|core|JVM version"
  "dmidecode|dmidecode|Y|core|server model, DIMM count/size/speed"
  "multipath|device-mapper-multipath|Y|core|LUNs, paths per LUN, path state"
  "multipathd|device-mapper-multipath|Y|core|effective multipath config"
  "dmsetup|device-mapper|Y|core|device-mapper tables"
  "pvs|lvm2|Y|core|LVM physical volumes"
  "lvs|lvm2|Y|core|LVM striping across LUNs"
  "nvme|nvme-cli|Y|core|local NVMe devices"
  "dlnkmgr|HDLM (Hitachi, optional)|Y|core|Hitachi HDLM path view"
)

print_deps() {
  local missing=0 noroot=0 st
  printf '%-11s %-9s %-26s %-5s %-7s %s\n' COMMAND STATUS "RHEL8 PACKAGE" ROOT MODE "USED FOR"
  printf '%-11s %-9s %-26s %-5s %-7s %s\n' ------- ------ ------------- ---- ---- --------
  for row in "${DEPS[@]}"; do
    IFS='|' read -r cmd pkg root mode use <<<"$row"
    if ! have "$cmd"; then st=MISSING; missing=$((missing+1))
    elif [[ $root == Y && $IS_ROOT -eq 0 ]]; then st=NO-ROOT; noroot=$((noroot+1))
    else st=found; fi
    printf '%-11s %-9s %-26s %-5s %-7s %s\n' "$cmd" "$st" "$pkg" "$root" "$mode" "$use"
  done
  local pyst="not found (summary.txt will be skipped)"
  [[ -n $PY ]] && pyst="found: $PY"
  printf '%-11s %s\n' "python" "optional, summary only — $pyst"
  echo
  echo "MISSING=$missing (skipped)   NO-ROOT=$noroot (skipped: needs root, running as $(id -un))"
  echo "Nothing is installed by this script. /proc and /sys are read directly."
}

# -----------------------------------------------------------------------------
# Python summariser (stdlib only, python 3.6 compatible). Reads OUTDIR/raw only,
# so it can be re-run anywhere with --summarize.
# -----------------------------------------------------------------------------
summarize() {
  local dir=$1
  if [[ -z $PY ]]; then echo "python not found — summary.txt skipped (raw data is complete)"; return 0; fi
  "$PY" - "$dir" >"$dir/summary.txt" 2>"$dir/summary.err" <<'PYEOF'
import os, re, sys, glob

D = sys.argv[1]; RAW = os.path.join(D, "raw")

def raw(name):
    p = os.path.join(RAW, name + ".txt")
    try:
        with open(p) as f: t = f.read()
    except IOError:
        return ""
    return "" if t.startswith("SKIPPED") else t

def kv_colon(text):
    out = {}
    for line in text.splitlines():
        if ":" in line:
            k, v = line.split(":", 1); out[k.strip()] = v.strip()
    return out

def bracket(v):
    m = re.search(r"\[([^\]]+)\]", v or ""); return m.group(1) if m else (v or "").strip()

def num(s, default=None):
    m = re.search(r"-?\d+(\.\d+)?", s or ""); return float(m.group(0)) if m else default

def gb(kb): return kb / 1024.0 / 1024.0

L = []
def h(title): L.append(""); L.append(title); L.append("-" * len(title))
def row(k, v):
    v = str(v).strip().splitlines()[0] if str(v).strip() else "?"
    L.append("  %-30s %s" % (k, v if len(v) <= 160 else v[:157] + "..."))

meta = kv_colon(raw("meta"))
L.append("MACHINE PROFILE — %s   (%s)" % (meta.get("host", "?"), meta.get("date", "?")))
L.append("collected as user: %s   root: %s" % (meta.get("user", "?"), meta.get("root", "?")))

# ---------------- platform ----------------
h("PLATFORM")
dmi = kv_colon(raw("dmi_id"))
def dmiv(k):
    for kk, vv in dmi.items():
        if kk.endswith("/" + k): return vv
    return "?"
row("Server", "%s %s" % (dmiv("sys_vendor"), dmiv("product_name")))
row("BIOS", "%s (%s)" % (dmiv("bios_version"), dmiv("bios_date")))
row("OS", raw("os_release").strip() or "?")
row("Kernel", raw("kernel").strip() or "?")
cmdline = raw("kernel_cmdline").strip()
row("Kernel cmdline flags", " ".join(w for w in cmdline.split()
    if re.match(r"(mitigations|transparent_hugepage|numa|isolcpus|nohz|intel_idle|processor\.max_cstate|elevator|hugepage)", w)) or "(none of interest)")
tuned = raw("tuned").strip()
row("tuned profile", tuned.replace("Current active profile:", "").strip() or "?")
irq = raw("irqbalance").strip()
row("irqbalance", irq or "?")
vul = raw("cpu_vulnerabilities")
if vul:
    nv = len([l for l in vul.splitlines() if "Vulnerable" in l])
    nm = len([l for l in vul.splitlines() if "Mitigation" in l])
    row("CPU vuln. mitigations", "%d mitigated, %d vulnerable (mitigations add syscall/IO overhead)" % (nm, nv))
row("Clocksource", raw("clocksource").strip() or "?")
row("cgroup fs", raw("cgroup").strip() or "?")

# ---------------- CPU ----------------
h("CPU")
c = kv_colon(raw("lscpu"))
sockets = int(num(c.get("Socket(s)"), 1)); cps = int(num(c.get("Core(s) per socket"), 1))
tpc = int(num(c.get("Thread(s) per core"), 1)); logical = int(num(c.get("CPU(s)"), 0))
nodes = int(num(c.get("NUMA node(s)"), 1)); phys = sockets * cps
row("Model", c.get("Model name", "?"))
row("Sockets x cores x threads", "%d x %d x %d  = %d physical cores, %d logical CPUs" % (sockets, cps, tpc, phys, logical))
row("NUMA nodes", nodes)
row("Clock (base / max MHz)", "%s / %s" % (c.get("CPU MHz", "?"), c.get("CPU max MHz", "?")))
row("L2 / L3 cache", "%s / %s" % (c.get("L2 cache", "?"), c.get("L3 cache", "?")))
flags = c.get("Flags", "")
row("AVX2 / AVX-512", "%s / %s" % ("yes" if " avx2" in " " + flags else "no", "yes" if "avx512f" in flags else "no"))
gov = raw("cpu_governor").strip()
row("CPU governor", gov or "not exposed")

# ---------------- memory / NUMA ----------------
h("MEMORY / NUMA")
mi = {}
for line in raw("meminfo").splitlines():
    p = line.split()
    if len(p) >= 2: mi[p[0].rstrip(":")] = float(p[1])
row("Total RAM", "%.0f GB" % gb(mi.get("MemTotal", 0)))
row("Available / Cached", "%.0f GB / %.0f GB" % (gb(mi.get("MemAvailable", 0)), gb(mi.get("Cached", 0))))
pernode = re.findall(r"node (\d+) size: (\d+) MB", raw("numactl_h"))
if pernode:
    row("RAM per NUMA node", ", ".join("n%s=%.0f GB" % (n, int(s) / 1024.0) for n, s in pernode))
dimms = raw("dmidecode_memory")
if dimms:
    devs = dimms.split("Memory Device")[1:]
    inst = [d for d in devs if re.search(r"^\s*Size:\s*\d", d, re.M)]
    total = 0.0
    for d in inst:
        m = re.search(r"^\s*Size:\s*(\d+)\s*(GB|MB|TB)", d, re.M)
        if m: total += float(m.group(1)) * {"MB": 1 / 1024.0, "GB": 1, "TB": 1024}[m.group(2)]
    speeds = sorted(set(re.findall(r"Configured (?:Memory|Clock) Speed:\s*(\d+ \S+)", "".join(inst))))
    row("DIMMs installed", "%d of %d slots, %.0f GB, speed %s" % (len(inst), len(devs), total, ", ".join(speeds) or "?"))
else:
    row("DIMMs", "not collected (dmidecode needs root)")
thp = kv_colon(raw("thp"))
thp_en = bracket(next((v for k, v in thp.items() if k.endswith("enabled")), ""))
row("Transparent huge pages", "enabled=%s defrag=%s" % (thp_en or "?", bracket(next((v for k, v in thp.items() if k.endswith("defrag")), "")) or "?"))
hp = mi.get("HugePages_Total", 0)
row("Static huge pages", "%d x %s kB" % (hp, int(mi.get("Hugepagesize", 0))))
sc = {}
for line in raw("sysctl").splitlines():
    if "=" in line:
        k, v = line.split("=", 1); sc[k.strip()] = v.strip()
row("swappiness / zone_reclaim", "%s / %s" % (sc.get("vm.swappiness", "?"), sc.get("vm.zone_reclaim_mode", "?")))
row("numa_balancing", sc.get("kernel.numa_balancing", "?"))
row("dirty ratio / background", "%s / %s" % (sc.get("vm.dirty_ratio", "?"), sc.get("vm.dirty_background_ratio", "?")))
row("max_map_count", sc.get("vm.max_map_count", "?"))
swap = [l for l in raw("swapon").splitlines()[1:] if l.strip()]
row("Swap devices", len(swap))
mmc = num(sc.get("vm.max_map_count"), 0)

# ---------------- FC ----------------
h("FIBRE CHANNEL")
fc = {}
for line in raw("fc_hosts").splitlines():
    p = line.split(None, 2)
    if len(p) == 3: fc.setdefault(p[0], {})[p[1]] = p[2]
speed_gb = {"8": 0.8, "16": 1.6, "32": 3.2, "64": 6.4}
online = 0; bw = 0.0
for host in sorted(fc):
    a = fc[host]; st = a.get("port_state", "?"); sp = a.get("speed", "?")
    row(host, "state=%-9s speed=%-12s wwpn=%s  link_fail=%s crc_err=%s" % (st, sp, a.get("port_name", "?"),
        a.get("link_failure_count", "?"), a.get("invalid_crc_count", "?")))
    if st == "Online":
        online += 1; m = re.match(r"(\d+)", sp); bw += speed_gb.get(m.group(1), 0) if m else 0
row("Online ports", "%d of %d  -> theoretical ~%.1f GB/s per direction" % (online, len(fc), bw))
qd = raw("hba_params").strip()
if qd: row("HBA driver queue params", "; ".join(l.split("/")[-1] for l in qd.splitlines()))

# ---------------- multipath / LUNs ----------------
h("MULTIPATH / LUNs")
mp = raw("multipath_ll")
maps = []; cur = None
for line in mp.splitlines():
    m = re.match(r"^(\S+)\s+\((\S+)\)\s+(dm-\d+)\s+(.+)$", line)
    if m:
        cur = {"name": m.group(1), "dm": m.group(3), "vp": m.group(4), "paths": 0, "active": 0, "size": "?", "policy": "?", "hosts": set()}
        maps.append(cur); continue
    if cur is None: continue
    m = re.search(r"size=(\S+)", line)
    if m: cur["size"] = m.group(1)
    m = re.search(r"policy='([^']+)'", line)
    if m: cur["policy"] = m.group(1)
    m = re.search(r"(\d+):\d+:\d+:\d+\s+(sd\w+)\s+\d+:\d+\s+(.*)$", line)
    if m:
        cur["paths"] += 1; cur["hosts"].add("host" + m.group(1))
        if "active" in m.group(3) and "ready" in m.group(3): cur["active"] += 1
if mp:
    row("Multipath LUNs", len(maps))
    for x in maps[:24]:
        row("  " + x["name"], "%s %s size=%s paths=%d active=%d via %s policy=%s" % (x["dm"], x["vp"], x["size"], x["paths"], x["active"], ",".join(sorted(x["hosts"])), x["policy"]))
    if len(maps) > 24: row("  ...", "%d more" % (len(maps) - 24))
else:
    row("Multipath", "not collected (needs root) or not configured")

# ---------------- block queues ----------------
h("BLOCK DEVICE QUEUES")
bq = []
lines = raw("block_queues").splitlines()
for line in lines[1:]:
    p = line.split("\t")
    if len(p) >= 11: bq.append(dict(zip(["dev", "vendor", "model", "qd", "nrreq", "ra", "maxsec", "maxhw", "rota", "nomerge", "sched"], p)))
sd_hit = [b for b in bq if b["dev"].startswith("sd") and "HITACHI" in b["vendor"].upper()]
dms = [b for b in bq if b["dev"].startswith("dm-")]
if sd_hit:
    row("Hitachi path devices (sd*)", len(sd_hit))
    row("  queue_depth values", ", ".join(sorted(set(b["qd"] for b in sd_hit))))
    row("  nr_requests values", ", ".join(sorted(set(b["nrreq"] for b in sd_hit))))
    row("  max_sectors_kb", ", ".join(sorted(set(b["maxsec"] for b in sd_hit))))
    row("  scheduler", ", ".join(sorted(set(bracket(b["sched"]) for b in sd_hit))))
if dms:
    row("dm devices", len(dms))
    row("  read_ahead_kb values", ", ".join(sorted(set(b["ra"] for b in dms), key=lambda s: num(s, 0))))
    row("  scheduler", ", ".join(sorted(set(bracket(b["sched"]) for b in dms))))

# ---------------- LVM / filesystems ----------------
h("LVM / FILESYSTEMS")
lv = [l.split("|") for l in raw("lvs").splitlines() if "|" in l]
striped = [x for x in lv if len(x) >= 4 and num(x[3], 1) > 1]
if lv:
    row("Logical volumes", "%d (%d striped)" % (len(lv), len(striped)))
    for x in striped[:10]: row("  " + x[0].strip(), "vg=%s size=%s stripes=%s stripe_size=%s" % (x[1].strip(), x[2].strip(), x[3].strip(), x[4].strip() if len(x) > 4 else "?"))
else:
    row("LVM", "not collected (needs root) or none")
data_mounts = []
for line in raw("findmnt").splitlines():
    p = line.split()
    if len(p) >= 4 and p[2] in ("xfs", "ext4") and not re.match(r"^/(boot|var|tmp|home|opt|usr)?$", p[0]):
        data_mounts.append(p)
for p in data_mounts[:15]:
    row(p[0], "%s on %s  %s" % (p[2], p[1], "noatime" if "noatime" in p[3] else "atime(relatime)"))

# ---------------- /dev/shm, NVMe ----------------
h("/dev/shm & LOCAL NVMe")
shm = raw("shm_df").splitlines()
row("/dev/shm", shm[-1].strip() if len(shm) > 1 else "?")
shm_m = raw("shm_mount").strip()
row("/dev/shm mount opts", shm_m.split()[3] if len(shm_m.split()) > 3 else "?")
nv = [l for l in raw("nvme_list").splitlines() if l.startswith("/dev/nvme")]
row("Local NVMe devices", len(nv) if raw("nvme_list") else "not collected (needs root / nvme-cli)")

# ---------------- network ----------------
h("NETWORK")
for line in raw("net").splitlines():
    p = line.split()
    if len(p) >= 3 and p[0] != "lo": row(p[0], "speed=%s Mb/s state=%s" % (p[1], p[2]))

# ---------------- java ----------------
h("JAVA / LIMITS")
jl = [l for l in raw("java").splitlines() if "version" in l and not l.startswith("Picked up")]
row("java", jl[0] if jl else "?")
def ulim(flag):
    m = re.search(r"-%s\)\s+(\S+)" % flag, raw("ulimit")); return m.group(1) if m else "?"
row("open files (ulimit -n)", ulim("n"))
row("max user procs (ulimit -u)", ulim("u"))
row("locked memory (ulimit -l)", ulim("l"))

skipped = [os.path.basename(p)[:-4] for p in glob.glob(os.path.join(RAW, "*.txt")) if open(p).read(9) == "SKIPPED"]
if skipped:
    L.append(""); L.append("Skipped checks (missing tool or needs root): " + ", ".join(sorted(skipped)))

print("\n".join(L))
PYEOF
  local rc=$?
  if [[ $rc -ne 0 ]]; then echo "summary failed (see $dir/summary.err) — raw data is still complete"; else rm -f "$dir/summary.err"; fi
  return 0
}

# --- modes that exit early ---------------------------------------------------
if [[ $CHECK_ONLY -eq 1 ]]; then print_deps; exit 0; fi
if [[ -n $SUMMARIZE_ONLY ]]; then summarize "$SUMMARIZE_ONLY"; cat "$SUMMARIZE_ONLY/summary.txt" 2>/dev/null; exit 0; fi

# -----------------------------------------------------------------------------
HOST=$(hostname -s 2>/dev/null || cat /proc/sys/kernel/hostname)
TS=$(date +%Y%m%d-%H%M%S)
BASE="${PROFILE_ROOT}/${HOST}"
OUTDIR="${BASE}/${TS}"
umask 022
RAW="$OUTDIR/raw"; REPORT="$OUTDIR/report.txt"
mkdir -p "$BASE" 2>/dev/null
[[ -e "$OUTDIR" ]] && { TS="${TS}-$$"; OUTDIR="${BASE}/${TS}"; RAW="$OUTDIR/raw"; REPORT="$OUTDIR/report.txt"; }
mkdir "$OUTDIR" && mkdir "$RAW" || { echo "ERROR: cannot create $OUTDIR"; exit 1; }
chmod 755 "$PROFILE_ROOT" "$BASE" 2>/dev/null

section() { printf '\n\n==================== %s ====================\n' "$*" >>"$REPORT"; echo "  $*"; }

# cap <id> <required-cmd> <root Y|N> "<command line>"
cap() {
  local id=$1 req=$2 root=$3; shift 3
  local f="$RAW/$id.txt" rc
  printf '\n### [%s] $ %s\n' "$id" "$*" >>"$REPORT"
  if ! have "$req"; then echo "SKIPPED: '$req' not found" >"$f"
  elif [[ $root == Y && $IS_ROOT -eq 0 ]]; then echo "SKIPPED: needs root" >"$f"
  else
    if have timeout; then timeout 180 bash -c "$*" >"$f" 2>&1; else bash -c "$*" >"$f" 2>&1; fi
    rc=$?; [[ $rc -ne 0 ]] && echo "(exit $rc)" >>"$f"
  fi
  cat "$f" >>"$REPORT"
}
# capfiles <id> <files...>  — "path: value" for each readable single-line file
capfiles() {
  local id=$1; shift; local f="$RAW/$id.txt"; : >"$f"
  for p in "$@"; do [[ -r "$p" ]] && printf '%s: %s\n' "$p" "$(tr '\n' ' ' <"$p" 2>/dev/null | sed 's/ *$//')" >>"$f"; done
  printf '\n### [%s] (files)\n' "$id" >>"$REPORT"; cat "$f" >>"$REPORT"
}
# capcat <id> <file>  — whole multi-line file
capcat() {
  local id=$1 p=$2; local f="$RAW/$id.txt"
  if [[ -r "$p" ]]; then cat "$p" >"$f" 2>&1; else echo "SKIPPED: $p not readable" >"$f"; fi
  printf '\n### [%s] %s\n' "$id" "$p" >>"$REPORT"; cat "$f" >>"$REPORT"
}

echo "Collecting machine profile into $OUTDIR/"
{
  echo "host: $(hostname -f 2>/dev/null || echo "$HOST")"
  echo "date: $(date -Is)"
  echo "user: $(id -un)"
  echo "root: $([[ $IS_ROOT -eq 1 ]] && echo yes || echo no)"
  echo "script: get-machine-profile.sh 1.5"
} >"$RAW/meta.txt"
{ echo "MACHINE PROFILE — RAW REPORT"; cat "$RAW/meta.txt"; } >"$REPORT"
[[ $IS_ROOT -eq 0 ]] && echo "WARNING: not root — root-only checks are skipped (see deps.txt)" | tee -a "$REPORT"

print_deps >"$OUTDIR/deps.txt"
section "0. DEPENDENCIES"; cat "$OUTDIR/deps.txt" >>"$REPORT"; tail -2 "$OUTDIR/deps.txt" | head -1

# -----------------------------------------------------------------------------
section "1. PLATFORM"
capcat   os_release /etc/redhat-release
uname -r >"$RAW/kernel.txt"; printf '\n### [kernel]\n' >>"$REPORT"; cat "$RAW/kernel.txt" >>"$REPORT"
capcat   kernel_cmdline /proc/cmdline
capfiles dmi_id /sys/class/dmi/id/sys_vendor /sys/class/dmi/id/product_name /sys/class/dmi/id/bios_version /sys/class/dmi/id/bios_date
cap      dmidecode_system dmidecode Y "dmidecode -t system | grep -E 'Manufacturer|Product Name|Version|Family'"
cap      tuned tuned-adm N "tuned-adm active"
cap      irqbalance systemctl N "systemctl is-active irqbalance"
cap      cpu_vulnerabilities grep N "grep -H . /sys/devices/system/cpu/vulnerabilities/* | sed 's|/sys/devices/system/cpu/vulnerabilities/||'"
capcat   clocksource /sys/devices/system/clocksource/clocksource0/current_clocksource
cap      cgroup stat N "stat -fc %T /sys/fs/cgroup"

section "2. CPU"
cap      lscpu lscpu N "lscpu"
cap      lscpu_e lscpu N "lscpu -e=CPU,NODE,SOCKET,CORE,ONLINE"
capcat   cpu_governor /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor

section "3. MEMORY / NUMA"
capcat   meminfo /proc/meminfo
cap      free free N "free -g"
cap      numactl_h numactl N "numactl -H"
cap      numastat_m numastat N "numastat -m | head -40"
cap      node_hugepages grep N "grep -H . /sys/devices/system/node/node*/hugepages/hugepages-*/nr_hugepages"
capfiles thp /sys/kernel/mm/transparent_hugepage/enabled /sys/kernel/mm/transparent_hugepage/defrag
cap      sysctl sysctl N "sysctl vm.swappiness vm.dirty_ratio vm.dirty_background_ratio vm.dirty_bytes vm.dirty_background_bytes vm.dirty_expire_centisecs vm.max_map_count vm.zone_reclaim_mode vm.overcommit_memory kernel.numa_balancing"
cap      swapon swapon N "swapon --show"
cap      dmidecode_memory dmidecode Y "dmidecode -t memory"

section "4. FIBRE CHANNEL"
cap      lspci_fc lspci N "lspci | grep -iE 'fibre|emulex|qlogic|hba'"
: >"$RAW/fc_hosts.txt"
for h in /sys/class/fc_host/host*; do
  [[ -d "$h" ]] || continue; hn=$(basename "$h")
  for a in port_state speed supported_speeds port_name node_name port_type fabric_name symbolic_name \
           statistics/link_failure_count statistics/loss_of_sync_count statistics/error_frames statistics/invalid_crc_count; do
    [[ -r "$h/$a" ]] && printf '%s %s %s\n' "$hn" "$(basename "$a")" "$(tr -d '\n' <"$h/$a" 2>/dev/null)" >>"$RAW/fc_hosts.txt"
  done
done
[[ -s "$RAW/fc_hosts.txt" ]] || echo "SKIPPED: no /sys/class/fc_host entries" >"$RAW/fc_hosts.txt"
printf '\n### [fc_hosts] /sys/class/fc_host/*\n' >>"$REPORT"; cat "$RAW/fc_hosts.txt" >>"$REPORT"
cap      systool_fc systool N "systool -c fc_host -v"
capfiles hba_params /sys/module/qla2xxx/parameters/ql2xmaxqdepth /sys/module/lpfc/parameters/lpfc_lun_queue_depth \
                    /sys/module/lpfc/parameters/lpfc_hba_queue_depth /sys/module/lpfc/parameters/lpfc_tgt_queue_depth \
                    /sys/module/qla2xxx/version /sys/module/lpfc/version

section "5. MULTIPATH / LUNs"
cap      multipath_ll multipath Y "multipath -ll"
cap      multipathd_paths multipathd Y "multipathd show paths format '%d %D %t %T %o %s %P %S'"
cap      multipathd_config multipathd Y "multipathd show config | grep -B2 -A30 -i 'HITACHI' | head -80"
cap      multipathd_service systemctl N "systemctl is-active multipathd"
capcat   multipath_conf /etc/multipath.conf
cap      multipath_confd cat N "cat /etc/multipath/conf.d/*.conf 2>/dev/null || echo none"
cap      dmsetup_table dmsetup Y "dmsetup table | grep -i multipath"
cap      hdlm dlnkmgr Y "dlnkmgr view -path"

section "6. BLOCK DEVICES"
cap      lsblk lsblk N "lsblk -o NAME,KNAME,SIZE,TYPE,VENDOR,MODEL,ROTA,SCHED,RA,PHY-SEC,MIN-IO,OPT-IO,MOUNTPOINT"
rd() { [[ -r "$1" ]] && tr -d ' \n' <"$1" || printf -- '-'; }
{
  printf 'dev\tvendor\tmodel\tqdepth\tnr_requests\tread_ahead_kb\tmax_sectors_kb\tmax_hw_sectors_kb\trotational\tnomerges\tscheduler\n'
  for d in /sys/block/sd* /sys/block/dm-* /sys/block/nvme*; do
    [[ -d "$d" ]] || continue
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(basename "$d")" \
      "$(rd "$d/device/vendor")" "$(rd "$d/device/model")" "$(rd "$d/device/queue_depth")" \
      "$(rd "$d/queue/nr_requests")" "$(rd "$d/queue/read_ahead_kb")" "$(rd "$d/queue/max_sectors_kb")" \
      "$(rd "$d/queue/max_hw_sectors_kb")" "$(rd "$d/queue/rotational")" "$(rd "$d/queue/nomerges")" \
      "$(cat "$d/queue/scheduler" 2>/dev/null)"
  done
} >"$RAW/block_queues.txt"
printf '\n### [block_queues] /sys/block/*\n' >>"$REPORT"; column -t -s $'\t' "$RAW/block_queues.txt" >>"$REPORT" 2>/dev/null || cat "$RAW/block_queues.txt" >>"$REPORT"
cap      udev_rules grep N "grep -lE 'queue_depth|scheduler|read_ahead|HITACHI|OPEN-V' /etc/udev/rules.d/* /usr/lib/udev/rules.d/* 2>/dev/null || echo none"

section "7. LVM / FILESYSTEMS"
cap      pvs pvs Y "timeout 60 pvs -o +pv_used"
cap      lvs lvs Y "timeout 60 lvs --noheadings --separator '|' -o lv_name,vg_name,lv_size,stripes,stripe_size,devices"
cap      df df N "df -hT -x tmpfs -x devtmpfs"
cap      findmnt findmnt N "findmnt -rno TARGET,SOURCE,FSTYPE,OPTIONS | grep -vE '^/(proc|sys|run|dev/pts|dev/mqueue|dev/hugepages)'"
if have xfs_info && have findmnt; then
  : >"$RAW/xfs_info.txt"
  for m in $(findmnt -rno TARGET -t xfs); do { echo "--- $m"; xfs_info "$m" 2>&1; } >>"$RAW/xfs_info.txt"; done
else echo "SKIPPED: xfs_info or findmnt not found" >"$RAW/xfs_info.txt"; fi
printf '\n### [xfs_info]\n' >>"$REPORT"; cat "$RAW/xfs_info.txt" >>"$REPORT"

section "8. /dev/shm & LOCAL NVMe"
cap      shm_df df N "df -BG --output=target,size,used,avail /dev/shm"
cap      shm_mount grep N "grep -E ' /dev/shm ' /proc/mounts"
cap      nvme_list nvme Y "nvme list"
cap      lsblk_d lsblk N "lsblk -d -o NAME,SIZE,ROTA,TRAN,MODEL"

section "9. NETWORK"
: >"$RAW/net.txt"
for n in /sys/class/net/*; do
  printf '%s %s %s\n' "$(basename "$n")" "$(cat "$n/speed" 2>/dev/null || echo -)" "$(cat "$n/operstate" 2>/dev/null)" >>"$RAW/net.txt"
done
printf '\n### [net] /sys/class/net/*\n' >>"$REPORT"; cat "$RAW/net.txt" >>"$REPORT"
cap      ip_link ip N "ip -br link"

section "10. JAVA / LIMITS"
cap      java java N "env -u JAVA_TOOL_OPTIONS -u _JAVA_OPTIONS -u JDK_JAVA_OPTIONS java -Xmx64m -version"
ulimit -a >"$RAW/ulimit.txt" 2>&1; printf '\n### [ulimit]\n' >>"$REPORT"; cat "$RAW/ulimit.txt" >>"$REPORT"
capfiles fs_limits /proc/sys/fs/file-max /proc/sys/fs/nr_open

# -----------------------------------------------------------------------------
section "SUMMARY"
summarize "$OUTDIR"
if have tar; then tar czf "${OUTDIR}.tar.gz" -C "$BASE" "$TS" 2>/dev/null && ARCHIVE="${OUTDIR}.tar.gz"; fi
# results read-only for everyone: files 444, dirs 555 (x needed to open a dir).
# Parent dirs 755: readable by all, writable only by the user who created them.
chmod 755 "$PROFILE_ROOT" "$BASE" 2>/dev/null
find "$OUTDIR" -type f -exec chmod 444 {} + 2>/dev/null
find "$OUTDIR" -type d -exec chmod 555 {} + 2>/dev/null
[[ -n "${ARCHIVE:-}" ]] && chmod 444 "$ARCHIVE" 2>/dev/null

echo
[[ -s "$OUTDIR/summary.txt" ]] && cat "$OUTDIR/summary.txt"
echo
echo "Done."
echo "  Folder : $OUTDIR/  (summary.txt, report.txt, deps.txt, raw/)"
[[ -n "${ARCHIVE:-}" ]] && echo "  Archive: $ARCHIVE  <- share this file"
exit 0
