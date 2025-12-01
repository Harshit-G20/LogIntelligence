#!/usr/bin/env python3
import time, random, json, os, argparse, datetime

# ----------------- Args -----------------
parser = argparse.ArgumentParser()

parser.add_argument("--rate", type=int, default=100, help="total lines/sec across all files")
parser.add_argument("--dir", default="/var/log/fake", help="output directory")
parser.add_argument("--rotate-seconds", type=int, default=86400, help="rotate files every N seconds")

parser.add_argument("--seconds", type=int, default=0, help="stop after N seconds (0=run forever)")
parser.add_argument("--max-lines", type=int, default=0, help="stop after N lines (0=unlimited)")

parser.add_argument("--ts-format", choices=["iso","bsd","rfc5424"], default="iso",
                    help="timestamp format: iso=RFC3339, bsd=RFC3164, rfc5424=syslog")

parser.add_argument("--hosts", default="web-1,web-2,auth-1,db-1",
                    help="comma-separated hostnames")

parser.add_argument("--output-format", choices=["text","json"], default="json",
                    help="Emit logs in plain text or structured JSON (recommended: json)")

parser.add_argument("--anomaly-profile",
                    choices=["off","portscan","auth-burst","db-error","latency-spike"],
                    default="off", help="inject anomalies")

parser.add_argument("--anomaly-interval", type=int, default=120, help="burst every N seconds")
parser.add_argument("--anomaly-duration", type=int, default=20, help="burst length")

args = parser.parse_args()

# ----------------- Globals -----------------
OUTDIR = args.dir
RATE = max(1, args.rate)

SPLIT = {"sys": 0.40, "net": 0.30, "app": 0.30}
COUNTS = {k: int(RATE * v) for k, v in SPLIT.items()}
diff = RATE - sum(COUNTS.values())
if diff > 0:
    COUNTS["sys"] += diff

BASENAMES = {"sys":"fake_syslog.log","net":"fake_network.log","app":"fake_app.log"}
HOSTS = [h.strip() for h in args.hosts.split(",") if h.strip()] or ["host-1"]

COMMON_TCP_DST = [80,443,22,25,110,143,3389,3306,5432,8080]
COMMON_UDP_DST = [53,123,161,500]
TCP_FLAGS = ["S","SA","A","FA","R","RA"]
APP_LEVELS = ["ERROR","WARN","INFO","DEBUG"]
SYS_PROGS  = ["sshd","cron","systemd","kernel","sudo","auth"]
SEV_ORDER  = ["CRIT","ERR","WARNING","NOTICE","INFO","DEBUG"]

# ----------------- Helpers -----------------
def ensure_dir():
    os.makedirs(OUTDIR, exist_ok=True)

def rotate_path(basename):
    ts = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")
    return os.path.join(OUTDIR, f"{basename}.{ts}")

def open_files():
    paths = {k: rotate_path(BASENAMES[k]) for k in BASENAMES}
    files = {k: open(paths[k], "a", buffering=1, encoding="utf-8") for k in paths}
    return files

def close_files(files):
    for f in files.values():
        try: f.close()
        except: pass

def now_iso():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def now_bsd():
    return datetime.datetime.now(datetime.timezone.utc).strftime("%b %d %H:%M:%S")

def now_rfc5424():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()

def ts():
    if args.ts_format == "iso": return now_iso()
    elif args.ts_format == "bsd": return now_bsd()
    else: return now_rfc5424()

def rand_ip():
    return ".".join(str(random.randint(1,254)) for _ in range(4))

def emit_json(obj):
    # Unified JSON schema
    obj["@timestamp"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
    return json.dumps(obj)

def in_burst(now, started_at, duration):
    return started_at is not None and now - started_at < duration

burst_started_at = None
next_burst_at = None

# ----------------- Generators -----------------

### SYSLOG
def gen_sys_json():
    host = random.choice(HOSTS)
    prog = random.choice(SYS_PROGS)
    pid  = random.randint(100, 9999)

    # ANOMALY
    if args.anomaly_profile == "auth-burst" and burst_started_at:
        return emit_json({
            "type": "sys",
            "host": host,
            "program": "sshd",
            "pid": pid,
            "severity": "WARNING",
            "message": "Failed password for invalid user",
            "src_ip": rand_ip(),
            "src_port": random.randint(1024, 65535)
        })

    # Normal logs
    msgs = [
        ("INFO",   lambda: f"Accepted publickey for user from {rand_ip()}"),
        ("WARNING",lambda: f"Failed password for invalid user from {rand_ip()} port {random.randint(1024,65535)}"),
        ("NOTICE", lambda: f"Connection closed by user root {rand_ip()}"),
        ("INFO",   lambda: "systemd: Started new session"),
        ("ERR",    lambda: "kernel: eth0: link is down"),
        ("WARNING",lambda: f"kernel: TCP: Possible SYN flood on port {random.choice([22,80,443,3306])}")
    ]

    sev, builder = random.choice(msgs)
    message = builder()

    return emit_json({
        "type": "sys",
        "host": host,
        "program": prog,
        "pid": pid,
        "severity": sev,
        "message": message
    })


def gen_sys_text():
    return gen_sys_json()  # Allow text to reuse JSON then stringify?
    # If you want real text mode, replace with your original text builder.


### NETWORK
def gen_net_json():
    proto = random.choices(["TCP","UDP","ICMP"], weights=[0.65,0.25,0.10], k=1)[0]

    # ANOMALY - port scan
    if args.anomaly_profile == "portscan" and burst_started_at:
        return emit_json({
            "type": "net",
            "proto": "TCP",
            "src_ip": rand_ip(),
            "src_port": random.randint(1024,65535),
            "dst_ip": rand_ip(),
            "dst_port": random.randint(1,65535),
            "size": 60,
            "flags": "S",
            "anomaly": "portscan"
        })

    # Normal traffic
    src_ip = rand_ip()
    dst_ip = rand_ip()

    if proto == "TCP":
        dport = random.choice(COMMON_TCP_DST) if random.random() < 0.6 else random.randint(1,65535)
        return emit_json({
            "type": "net",
            "proto": "TCP",
            "src_ip": src_ip,
            "src_port": random.randint(1024,65535),
            "dst_ip": dst_ip,
            "dst_port": dport,
            "flags": random.choice(TCP_FLAGS),
            "size": random.randint(60,1500)
        })

    elif proto == "UDP":
        dport = random.choice(COMMON_UDP_DST) if random.random()<0.6 else random.randint(1,65535)
        return emit_json({
            "type": "net",
            "proto": "UDP",
            "src_ip": src_ip,
            "src_port": random.randint(1024,65535),
            "dst_ip": dst_ip,
            "dst_port": dport,
            "size": random.randint(60,1500)
        })

    else: # ICMP
        t,c = random.choice([(8,0),(0,0),(3,1),(11,0)])
        return emit_json({
            "type": "net",
            "proto": "ICMP",
            "src_ip": src_ip,
            "dst_ip": dst_ip,
            "icmp_type": t,
            "icmp_code": c,
            "size": random.randint(32,128)
        })


def gen_net_text():
    return gen_net_json()


### APP LOGS
def gen_app_json():
    level = random.choice(APP_LEVELS)
    reqid = f"req-{random.randint(100000, 999999)}"

    payload = {
        "type": "app",
        "host": random.choice(HOSTS),
        "level": level,
        "request_id": reqid,
        "message": random.choice([
            "User login success",
            "User login failed",
            "Payment processed",
            "Cache miss",
            "DB connection error",
            "Background job completed"
        ]),
        "lat_ms": random.randint(1,500)
    }

    # ANOMALIES
    if args.anomaly_profile == "db-error" and burst_started_at:
        payload["level"] = "ERROR"
        payload["message"] = "DB connection error"

    if args.anomaly_profile == "latency-spike" and burst_started_at:
        payload["level"] = "WARN"
        payload["message"] = "Slow request"
        payload["lat_ms"] = random.randint(800,1600)

    return emit_json(payload)


def gen_app_text():
    return gen_app_json()

# ----------------- Main Loop -----------------

def run():
    global burst_started_at, next_burst_at
    ensure_dir()
    start_rotate = time.time()
    files = open_files()
    deadline = time.time() + args.seconds if args.seconds > 0 else None
    emitted_total = 0

    if args.anomaly_profile != "off":
        next_burst_at = time.time() + args.anomaly_interval

    try:
        while True:
            now = time.time()

            # Anomaly timing
            if args.anomaly_profile != "off" and burst_started_at is None and next_burst_at and now >= next_burst_at:
                burst_started_at = now
                next_burst_at = now + args.anomaly_interval

            if burst_started_at and not in_burst(now, burst_started_at, args.anomaly_duration):
                burst_started_at = None

            # Rotate files
            if time.time() - start_rotate >= args.rotate_seconds:
                close_files(files)
                files = open_files()
                start_rotate = time.time()

            # Generate in small batches
            batches = 10
            per_batch = {k: max(0, (COUNTS[k] + batches - 1)//batches) for k in COUNTS}

            for _ in range(batches):
                batch_start = time.time()

                for k in ("sys","net","app"):
                    gen = {
                        ("sys","json"): gen_sys_json,
                        ("sys","text"): gen_sys_text,
                        ("net","json"): gen_net_json,
                        ("net","text"): gen_net_text,
                        ("app","json"): gen_app_json,
                        ("app","text"): gen_app_text,
                    }[(k,args.output_format)]

                    for _ in range(per_batch[k]):
                        line = gen()
                        files[k].write(line + "\n")
                        emitted_total += 1

                        if (deadline and time.time() >= deadline) or (args.max_lines and emitted_total >= args.max_lines):
                            raise KeyboardInterrupt

                elapsed = time.time() - batch_start
                time.sleep(max(0, (1.0/batches) - elapsed))

    except KeyboardInterrupt:
        pass
    finally:
        close_files(files)

if __name__ == "__main__":
    run()
