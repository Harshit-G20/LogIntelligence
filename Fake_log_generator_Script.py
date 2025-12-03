#!/usr/bin/env python3
import time, random, json, os, argparse, datetime

# ----------------- Args -----------------
parser = argparse.ArgumentParser()
parser.add_argument("--rate", type=int, default=100, help="total lines/sec across all files")
parser.add_argument("--dir", default="/var/log/fake", help="output directory")
parser.add_argument("--rotate-seconds", type=int, default=86400)
parser.add_argument("--seconds", type=int, default=0)
parser.add_argument("--max-lines", type=int, default=0)
parser.add_argument("--ts-format", choices=["iso","bsd","rfc5424"], default="iso")
parser.add_argument("--hosts", default="web-1,web-2,auth-1,db-1")
parser.add_argument("--anomaly-profile", choices=["off","portscan","auth-burst","db-error","latency-spike"],
                    default="off")
parser.add_argument("--anomaly-interval", type=int, default=120)
parser.add_argument("--anomaly-duration", type=int, default=20)
args = parser.parse_args()

OUTDIR = args.dir
RATE = max(1, args.rate)
SPLIT = {"sys": 0.40, "net": 0.30, "app": 0.30}
COUNTS = {k: int(RATE * v) for k, v in SPLIT.items()}
diff = RATE - sum(COUNTS.values())
if diff > 0: COUNTS["sys"] += diff

BASENAMES = {"sys":"fake_syslog.log","net":"fake_network.log","app":"fake_app.log"}
HOSTS = [h.strip() for h in args.hosts.split(",") if h.strip()] or ["host-1"]

COMMON_TCP_DST = [80,443,22,25,110,143,3389,3306,5432,8080]
COMMON_UDP_DST = [53,123,161,500]
TCP_FLAGS = ["S","SA","A","FA","R","RA"]
APP_LEVELS = ["ERROR","WARN","INFO","DEBUG"]
SYS_PROGS  = ["sshd","cron","systemd","kernel","sudo","auth"]

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

def ts_iso(): return datetime.datetime.now(datetime.timezone.utc).isoformat()

def rand_ip():
    return ".".join(str(random.randint(1,254)) for _ in range(4))

burst_started_at = None
next_burst_at = None

# ---------- JSON Generators ----------
def gen_sysline():
    host = random.choice(HOSTS)
    pid = random.randint(100, 9999)

    sev = random.choice(["INFO","WARNING","NOTICE","ERR"])
    message = random.choice([
        "User login successful",
        "Failed password attempt",
        "Cron job executed",
        "Kernel driver reset",
        "User session started",
        "User session terminated"
    ])

    if args.anomaly_profile == "auth-burst" and burst_started_at:
        sev = "ERROR"
        message = f"Failed password for invalid user from {rand_ip()}"

    payload = {
        "@timestamp": ts_iso(),
        "log_type": "sys",
        "host": host,
        "severity": sev,
        "process": random.choice(SYS_PROGS),
        "pid": pid,
        "message": message
    }
    return json.dumps(payload)

def gen_netline():
    proto = random.choices(["TCP","UDP","ICMP"], weights=[0.65,0.25,0.10], k=1)[0]
    src_ip = rand_ip()
    dst_ip = rand_ip()
    flags = ""
    port = None

    if proto == "TCP":
        port = random.choice(COMMON_TCP_DST)
        flags = random.choice(TCP_FLAGS)

    if args.anomaly_profile == "portscan" and burst_started_at:
        port = random.randint(1, 65000)

    payload = {
        "@timestamp": ts_iso(),
        "log_type": "net",
        "protocol": proto,
        "src_ip": src_ip,
        "dst_ip": dst_ip,
        "port": port,
        "flags": flags,
        "size": random.randint(32,1500)
    }
    return json.dumps(payload)

def gen_appline():
    level = random.choice(APP_LEVELS)
    reqid = f"req-{random.randint(100000, 999999)}"
    msg = random.choice([
        "User login success",
        "User login failed",
        "Payment processed",
        "Cache miss",
        "DB connection error",
    ])

    payload = {
        "@timestamp": ts_iso(),
        "log_type": "app",
        "host": random.choice(HOSTS),
        "level": level,
        "request_id": reqid,
        "message": msg,
        "lat_ms": random.randint(1, 800)
    }

    if burst_started_at:
        if args.anomaly_profile == "db-error":
            payload["level"] = "ERROR"
            payload["message"] = "Error connecting to DB"

        elif args.anomaly_profile == "latency-spike":
            payload["level"] = "WARN"
            payload["message"] = "Slow request"
            payload["lat_ms"] = random.randint(800, 2000)

    return json.dumps(payload)

# ----------------- Main loop -----------------
def run():
    global burst_started_at, next_burst_at
    ensure_dir()
    files = open_files()
    start_rotate = time.time()
    deadline = time.time() + args.seconds if args.seconds > 0 else None
    emitted_total = 0

    if args.anomaly_profile != "off":
        next_burst_at = time.time() + args.anomaly_interval

    try:
        while True:
            now = time.time()
            if args.anomaly_profile != "off" and burst_started_at is None and next_burst_at and now >= next_burst_at:
                burst_started_at = now
                next_burst_at = now + args.anomaly_interval
            if burst_started_at and now - burst_started_at >= args.anomaly_duration:
                burst_started_at = None

            if time.time() - start_rotate >= args.rotate_seconds:
                close_files(files)
                files = open_files()
                start_rotate = time.time()

            for k in ("sys","net","app"):
                gen = gen_sysline if k=="sys" else gen_netline if k=="net" else gen_appline
                for _ in range(COUNTS[k]):
                    files[k].write(gen()+"\n")
                    emitted_total += 1
                    if (deadline and time.time() >= deadline) or (args.max_lines and emitted_total >= args.max_lines):
                        raise KeyboardInterrupt

            time.sleep(1)

    except KeyboardInterrupt:
        pass
    finally:
        close_files(files)


if __name__ == "__main__":
    run()
