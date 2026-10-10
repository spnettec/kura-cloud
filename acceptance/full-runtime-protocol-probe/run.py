#!/usr/bin/env python3
"""Run the protocol probe and loopback broker in separate, owned JVMs."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import signal
import socket
import subprocess
import time


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def ports_available():
    for port in (18480, 18443, 18444):
        with socket.socket() as handle:
            handle.bind(("127.0.0.1", port))


def stop(process, graceful_stdin=False):
    if process.poll() is not None:
        return False
    if graceful_stdin:
        process.stdin.close()
    else:
        os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=15)
        return False
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)
        return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("runtime", "template-profile", "fixtures", "archive", "java", "core-repository"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    for name in vars(args):
        setattr(args, name, getattr(args, name).resolve())
    protected = Path.home() / ".kura-dev"
    if args.archive == protected or protected in args.archive.parents:
        parser.error("Use a new acceptance archive outside the personal profile")
    args.archive.mkdir(parents=True, exist_ok=False)
    ports_available()
    module = Path(__file__).resolve().parent
    helpers = args.archive / "helpers"
    helpers.mkdir()
    fixture_files = ["apiguardian-api.jar", "opentest4j.jar", "junit-platform-commons.jar",
                     "junit-jupiter-api.jar", "kura-configuration-test-fixtures.jar"]
    for file in fixture_files:
        shutil.copy2(args.fixtures / file, helpers / file)
    shutil.copy2(module / "target/kura-full-runtime-protocol-acceptance-1.0.0-SNAPSHOT.jar", helpers / "protocol-probe.jar")
    home = args.archive / "profile"
    shutil.copytree(args.template_profile, home, ignore=shutil.ignore_patterns("logs", "tmp", "*-result.json"))
    (home / "logs").mkdir()
    (home / "tmp").mkdir()
    relocated = []
    for file in home.rglob("*"):
        if file.is_file() and file.suffix in (".xml", ".properties", ".json"):
            raw = file.read_bytes()
            if str(args.template_profile).encode() in raw:
                file.write_bytes(raw.replace(str(args.template_profile).encode(), str(home).encode()))
                relocated.append(str(file.relative_to(home)))
    (home / ".protocol-acceptance-owned").write_text("Complete Mac protocol acceptance\n")
    (args.archive / "relocated-profile-files.json").write_text(json.dumps(relocated, indent=2) + "\n")
    configuration = args.archive / "configuration"
    configuration.mkdir()
    for file in (args.runtime / "configuration").iterdir():
        if file.is_file():
            text = file.read_text().replace(str(args.template_profile), str(home))
            text = text.replace(str(args.runtime / "configuration"), str(configuration))
            if file.name == "kura.properties":
                text = re.sub(r"(?m)^kura.snapshots.encrypt=.*$", "kura.snapshots.encrypt=true", text)
            if file.name == "config.ini":
                extra = ["reference:" + (helpers / f).as_uri() + "@5:start" for f in fixture_files]
                extra.append("reference:" + (helpers / "protocol-probe.jar").as_uri() + "@6:start")
                text = re.sub(r"(?m)^osgi.bundles=(.*)$", lambda m: m.group(0) + "," + ",".join(extra), text)
            (configuration / file.name).write_text(text)
    classpath = str(module / "target/test-classes") + ":" + (module / "target/broker-classpath.txt").read_text().strip()
    endpoint = args.archive / "broker.json"
    broker_command = [str(args.java), "-cp", classpath,
                      "org.eclipse.kura.cloud.testing.fullruntime.AcceptanceBroker", str(endpoint)]
    processes = []
    started = time.monotonic()
    result = {"passed": False}
    forced = []
    with (args.archive / "broker.log").open("w") as broker_log, (args.archive / "console.log").open("w") as app_log:
        broker = subprocess.Popen(broker_command, stdin=subprocess.PIPE, stdout=broker_log, stderr=subprocess.STDOUT, start_new_session=True)
        processes.append(broker)
        try:
            deadline = time.monotonic() + 15
            while broker.poll() is None and not endpoint.exists() and time.monotonic() < deadline:
                time.sleep(0.1)
            if not endpoint.exists():
                raise RuntimeError("No broker endpoint before exit/deadline")
            uri = json.loads(endpoint.read_text())["uri"]
            options = [x.replace(str(args.template_profile), str(home)).replace(str(args.runtime / "configuration"), str(configuration))
                       for x in shlex.split((args.runtime / "jvm.args").read_text())]
            command = [str(args.java), *options, "-Dkura.acceptance.root=" + str(args.archive),
                       "-Dkura.acceptance.broker=" + uri, "-jar", str(args.runtime / "launcher.jar"),
                       "-configuration", str(configuration), "-install", str(args.runtime), "-console", "-consoleLog"]
            (args.archive / "command.json").write_text(json.dumps(command, indent=2) + "\n")
            application = subprocess.Popen(command, cwd=args.runtime, stdin=subprocess.PIPE,
                                           stdout=app_log, stderr=subprocess.STDOUT, start_new_session=True)
            processes.append(application)
            result_file = home / "protocol-probe-result.json"
            deadline = time.monotonic() + 120
            while application.poll() is None and not result_file.exists() and time.monotonic() < deadline:
                time.sleep(0.25)
            result = json.loads(result_file.read_text()) if result_file.exists() else {
                "passed": False, "error": "No probe result before exit/120-second deadline"}
        finally:
            for process in reversed(processes):
                forced.append(stop(process, graceful_stdin=process is broker))
                if process.stdin and not process.stdin.closed:
                    process.stdin.close()
    ports_available()
    result.update(elapsedSeconds=round(time.monotonic() - started, 3), pids=[p.pid for p in processes],
                  exitCodes=[p.returncode for p in processes], forcedShutdown=any(forced), portsReleased=True)
    if any(forced):
        result.update(passed=False, error="Owned process required forced shutdown")
    final_broker = json.loads((args.archive / "broker-final.json").read_text())
    if result.get("passed"):
        if not set([result["clientId"], result["observerId"]]).issubset(final_broker["authenticatedClients"]):
            result.update(passed=False, error="Broker did not authenticate both actual clients")
    result["broker"] = final_broker
    result["helpers"] = {p.name: sha(p) for p in helpers.iterdir()}
    result["logs"] = {f: sha(args.archive / f) for f in ("console.log", "broker.log")}
    result["sources"] = {str(p.relative_to(module)): sha(p) for p in module.rglob("*")
                         if p.is_file() and "target" not in p.relative_to(module).parts}
    result["coreSources"] = {f: sha(args.core_repository / f) for f in [
        "build-support/kura-configuration-test-fixtures/src/main/java/org/eclipse/kura/testing/configuration/fixture/LegacyCoreScenarios.java",
        "build-support/kura-configuration-test-fixtures/src/main/java/org/eclipse/kura/testing/configuration/fixture/LegacyInventoryScenarios.java"]}
    (args.archive / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    print(json.dumps({k: result.get(k) for k in ("passed", "error", "elapsedSeconds", "forcedShutdown", "portsReleased")}), flush=True)
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
