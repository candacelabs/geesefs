#!/usr/bin/env python3
"""Compare multipart request timeouts against a live IDrive e2 test bucket.

Run in a disposable Linux container with Python 3.11+, curl, a GeeseFS binary,
and permission to mount FUSE. Provide the regional HTTPS endpoint, matching
region, and writable test bucket. Supply credentials as AWS_ACCESS_KEY_ID and
AWS_SECRET_ACCESS_KEY. Each run uses and removes its own object prefix.
Use --verify to finish and checksum the file instead of stopping after three
part acknowledgments or a signature error.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import random
import subprocess
import sys
import tempfile
import time
from urllib.parse import urlencode, urlsplit
from uuid import uuid4
from xml.etree import ElementTree

MIB = 1024**2


def block(index):
    return hashlib.shake_256(index.to_bytes(4, "big")).digest(MIB)


def write(path, size, verify):
    order = list(range(size))
    random.Random(75).shuffle(order)
    with path.open("w+b", buffering=0) as file:
        file.truncate(size * MIB)
        for index in order:
            assert os.pwrite(file.fileno(), block(index), index * MIB) == MIB
        if not verify:
            # Keep the file open until the observer has sampled the requests.
            time.sleep(3600)
        assert os.pread(file.fileno(), MIB, order[0] * MIB) == block(order[0])
        os.fsync(file.fileno())
    complete = path.parent.parent / "complete"
    complete.mkdir()
    path.rename(complete / "example.bin")
    directory = os.open(complete, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def clean(curl, bucket, prefix):
    for element, query in [("Upload", "uploads&"), ("Contents", "list-type=2&")]:
        url = bucket + "?" + query + urlencode({"prefix": prefix})
        xml = ElementTree.fromstring(subprocess.check_output([*curl, url]))
        for item in xml.findall("{*}" + element):
            key = item.findtext("{*}Key")
            assert key.startswith(prefix)
            target = bucket + "/" + key
            if element == "Upload":
                target += "?" + urlencode({"uploadId": item.findtext("{*}UploadId")})
            subprocess.run([*curl, "-X", "DELETE", target], check=True)
        xml = ElementTree.fromstring(subprocess.check_output([*curl, url]))
        assert not xml.findall("{*}" + element)


def observe(log, writer, seconds, verify, deadline):
    events = {
        "Flushing part ": "start part",
        "Flushed part ": "S3 acknowledged part",
        "SignatureDoesNotMatch": "S3 signature rejection",
        "Client.Timeout": "HTTP client deadline exceeded",
        "Retrying Request s3/UploadPart": "SDK retrying UploadPart",
    }
    start = time.monotonic()
    shown = 0
    while time.monotonic() - start < deadline:
        content = log.read_text(errors="replace")
        lines = content.splitlines()
        for line in lines[shown:]:
            for pattern, label in events.items():
                if pattern in line:
                    if pattern in ("Flushing part ", "Flushed part "):
                        label += " " + line.split(pattern, 1)[1].split(" of ", 1)[0]
                    print(f"{seconds}s +{time.monotonic() - start:.1f}s {label}", flush=True)
                    break
        shown = len(lines)
        status = writer.poll()
        if status is not None:
            assert verify and status == 0
            return content
        if not verify and ("SignatureDoesNotMatch" in content or content.count("Flushed part ") >= 3):
            return content
        time.sleep(0.25)
    raise TimeoutError(f"{seconds}s probe exceeded its observation deadline")


def probe(args, seconds, scratch, curl):
    bucket = args.endpoint.rstrip("/") + "/" + args.bucket
    prefix = f"geesefs-timeout-{seconds}-{uuid4().hex}/"
    mount = scratch / "mount"
    mount.mkdir()
    log = scratch / "geesefs.log"
    command = [
        args.geesefs, "-f", "--debug", "--debug_s3",
        "--endpoint", args.endpoint, "--region", args.region, "--list-type", "2",
        "--no-detect", "--no-expire-multipart", "--http-timeout", f"{seconds}s",
        "--memory-limit", str(args.cache_mib), "--max-flushers", "2",
        "--max-parallel-parts", "1", "--sdk-max-retries", "1", "--retry-interval", "2s",
        "--fsync-on-close", "--setuid", str(os.getuid()), "--setgid", str(os.getgid()),
        args.bucket + ":" + prefix.rstrip("/"), str(mount),
    ]
    writer = None
    with log.open("w") as output:
        geesefs = subprocess.Popen(command, stdout=output, stderr=output)
        try:
            for _ in range(100):
                if mount.is_mount():
                    break
                if geesefs.poll() is not None:
                    raise RuntimeError("GeeseFS exited before mounting")
                time.sleep(0.1)
            assert mount.is_mount()
            (mount / "incomplete").mkdir()
            path = mount / "incomplete/example.bin.part"
            writer = subprocess.Popen([
                sys.executable, __file__, "write", str(path), str(args.size_mib), str(int(args.verify)),
            ])
            content = observe(log, writer, seconds, args.verify, args.deadline)
            print(f"{seconds}s: {content.count('Flushed part ')} part acknowledgments, "
                  f"{content.count('SignatureDoesNotMatch')} signature log entries, "
                  f"{content.count('Client.Timeout')} client timeout log entries")
            if args.verify:
                expected = hashlib.sha256()
                for index in range(args.size_mib):
                    expected.update(block(index))
                url = bucket + "/" + prefix + "complete/example.bin"
                with subprocess.Popen([*curl, url], stdout=subprocess.PIPE) as reader:
                    actual = hashlib.file_digest(reader.stdout, "sha256").hexdigest()
                    assert reader.wait() == 0
                assert actual == expected.hexdigest()
                print("Renamed object SHA-256 matched")
        finally:
            if writer is not None and writer.poll() is None:
                writer.terminate()
            if mount.is_mount():
                subprocess.run(["umount", "-l", str(mount)], check=True)
            geesefs.terminate()
            try:
                geesefs.wait(timeout=5)
            except subprocess.TimeoutExpired:
                geesefs.kill()
                geesefs.wait()
            if writer is not None:
                writer.wait(timeout=5)
            clean(curl, bucket, prefix)
            print(f"{seconds}s test prefix clean")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--region", required=True)
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--geesefs", default="geesefs")
    parser.add_argument("--timeouts", nargs="+", type=int, default=[15, 60])
    parser.add_argument("--size-mib", type=int, default=96)
    parser.add_argument("--cache-mib", type=int, default=32)
    parser.add_argument("--deadline", type=int, default=150)
    parser.add_argument("--verify", action="store_true")
    args = parser.parse_args()
    endpoint = urlsplit(args.endpoint)
    if endpoint.scheme != "https" or not endpoint.hostname or not endpoint.hostname.endswith(".idrivee2.com"):
        parser.error("provide an IDrive e2 regional HTTPS endpoint")
    if args.size_mib <= args.cache_mib or args.cache_mib <= 0 or min(args.timeouts) <= 0:
        parser.error("size must exceed the cache and timeouts must be positive")

    os.umask(0o077)
    os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
    with tempfile.TemporaryDirectory(prefix="geesefs-idrive-", dir="/dev/shm") as temp:
        scratch = Path(temp)
        credentials = scratch / "curl.conf"
        credentials.write_text("user = " + json.dumps(
            os.environ["AWS_ACCESS_KEY_ID"] + ":" + os.environ["AWS_SECRET_ACCESS_KEY"]
        ) + "\n")
        curl = ["curl", "--disable", "-fsS", "--connect-timeout", "5", "--max-time", "120",
                "--config", str(credentials), "--aws-sigv4", f"aws:amz:{args.region}:s3"]
        for index, seconds in enumerate(args.timeouts):
            trial = scratch / str(index)
            trial.mkdir()
            probe(args, seconds, trial, curl)


if __name__ == "__main__":
    if sys.argv[1:2] == ["write"]:
        write(Path(sys.argv[2]), int(sys.argv[3]), bool(int(sys.argv[4])))
    else:
        main()
