"""Operational point-in-time recovery using pinned snapshots and committed journals."""

import hashlib
import json
import sqlite3
from pathlib import Path

STARTER = {"recovery.json": '{"snapshot_id":"snapshot-40","through_sequence":60}\n'}
RESULT_FILES = ["results/recovered.sqlite", "results/recovery-manifest.json"]
SPEC = """Recover a ticket service to the exact committed target_sequence in the pinned fixture. Edit only recovery.json: {"snapshot_id": "snapshot-10" or "snapshot-20" or "snapshot-40", "through_sequence": integer}. Any valid snapshot not newer than target_sequence is acceptable, not only the newest. Choose from snapshot manifests and checksum integrity, then set the replay cutoff. All later writes must be excluded.
The controlled executor will validate snapshot integrity, replay complete committed transactions contiguously, apply upserts/deletes atomically, and write results/recovered.sqlite plus recovery-manifest.json. It will deduplicate byte-identical journal overlap; conflicting duplicates, missing history or corruption at/before the selected replay cutoff will be rejected without publishing results. A truncated uncommitted tail after the target will be ignored. Empty target_sequence=0 uses snapshot-0. The invalid fixture has an unrecoverable missing transaction before the target and must be refused with no result files.
This is application-journal recovery, not binary SQLite WAL recovery. No code or evaluator editing is exposed. inspect_task shows the snapshot/log manifests and target; edit_files chooses the plan; assess performs restoration and independent state inspection. Submit the final assessment exactly."""


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def fixture(scenario, seed):
    transactions = []
    state = {}
    snapshots = []
    for seq in range(61):
        if seq:
            key = (seq + seed) % 9 + 1
            operations = [
                {
                    "op": "put",
                    "id": key,
                    "subject": f"Ticket {seed}-{seq}",
                    "status": "closed" if seq % 4 == 0 else "open",
                }
            ]
            if seq % 5 == 0:
                operations.append({"op": "delete", "id": (key + 2) % 9 + 1})
            entry = {"sequence": seq, "operations": operations, "committed": True}
            entry["sha256"] = hashlib.sha256(canonical(entry).encode()).hexdigest()
            transactions.append(entry)
            for op in operations:
                if op["op"] == "put":
                    state[op["id"]] = [op["id"], op["subject"], op["status"]]
                else:
                    state.pop(op["id"], None)
        if seq in (0, 10, 20, 40):
            rows = sorted(state.values())
            snapshots.append(
                {
                    "id": f"snapshot-{seq}",
                    "sequence": seq,
                    "tickets": rows,
                    "sha256": hashlib.sha256(canonical(rows).encode()).hexdigest(),
                }
            )
    journal = list(transactions)
    if scenario == "boundary":
        snapshots[2]["tickets"][0][1] = "CORRUPTED SNAPSHOT"
        journal = transactions[:25] + [transactions[24]] + transactions[25:]
    target = 0 if scenario == "empty" else (20 if scenario == "boundary" else 37)
    if scenario == "invalid":
        journal = [t for t in journal if t["sequence"] != 31]
    return {
        "scenario": scenario,
        "seed": seed,
        "target_sequence": target,
        "snapshots": snapshots,
        "journal": journal,
        "tail": {"sequence": 61, "committed": False, "partial": "truncated"},
        "history": transactions,
    }


def measure(files, raw, workspace):
    results = Path(workspace) / "results"
    results.mkdir(exist_ok=True)
    for name in ("recovered.sqlite", "recovery-manifest.json"):
        p = results / name
        if p.exists():
            p.unlink()
    try:
        plan = json.loads(files["recovery.json"])
        if set(plan) != {"snapshot_id", "through_sequence"} or type(plan["through_sequence"]) is not int:
            raise ValueError("malformed recovery plan")
        cutoff = plan["through_sequence"]
        if cutoff != raw["target_sequence"]:
            raise ValueError("wrong recovery target")
        chosen = next(s for s in raw["snapshots"] if s["id"] == plan["snapshot_id"])
        if chosen["sequence"] > cutoff:
            raise ValueError("snapshot is later than target")
        if hashlib.sha256(canonical(chosen["tickets"]).encode()).hexdigest() != chosen["sha256"]:
            raise ValueError("snapshot checksum failed")
        records = {r[0]: r for r in chosen["tickets"]}
        selected = {}
        for transaction in raw["journal"]:
            seq = transaction["sequence"]
            if not chosen["sequence"] < seq <= cutoff:
                continue
            clean = {k: v for k, v in transaction.items() if k != "sha256"}
            if (
                not transaction["committed"]
                or hashlib.sha256(canonical(clean).encode()).hexdigest() != transaction["sha256"]
            ):
                raise ValueError("journal integrity failed")
            if seq in selected and selected[seq] != transaction:
                raise ValueError("conflicting journal overlap")
            selected[seq] = transaction
        if sorted(selected) != list(range(chosen["sequence"] + 1, cutoff + 1)):
            raise ValueError("journal has a gap")
        # Materialize a database and apply each selected transaction through real SQLite commits.
        temp = results / "restore-working.sqlite"
        if temp.exists():
            temp.unlink()
        conn = sqlite3.connect(temp)
        conn.execute(
            "CREATE TABLE tickets(id INTEGER PRIMARY KEY,subject TEXT NOT NULL,status TEXT NOT NULL CHECK(status IN ('open','closed')))"
        )
        conn.executemany("INSERT INTO tickets VALUES (?,?,?)", sorted(records.values()))
        conn.commit()
        for seq in sorted(selected):
            with conn:
                for op in selected[seq]["operations"]:
                    if op["op"] == "put":
                        conn.execute(
                            "INSERT INTO tickets VALUES (?,?,?) ON CONFLICT(id) DO UPDATE SET subject=excluded.subject,status=excluded.status",
                            (op["id"], op["subject"], op["status"]),
                        )
                    elif op["op"] == "delete":
                        conn.execute("DELETE FROM tickets WHERE id=?", (op["id"],))
                    else:
                        raise ValueError("unknown journal operation")
        conn.close()
        temp.replace(results / "recovered.sqlite")
        manifest = {
            "snapshot_id": chosen["id"],
            "last_committed_sequence": cutoff,
            "replayed_transactions": len(selected),
        }
        (results / "recovery-manifest.json").write_text(canonical(manifest))
        conn = sqlite3.connect(results / "recovered.sqlite")
        rows = [list(r) for r in conn.execute("SELECT id,subject,status FROM tickets ORDER BY id")]
        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        # A frozen service's record-read/health operations observe the recovered DB.
        service = {
            "health": integrity == "ok",
            "ticket_count": conn.execute("SELECT COUNT(*) FROM tickets").fetchone()[0],
        }
        conn.close()
        return {
            "tickets": rows,
            "integrity": integrity,
            "cutoff": manifest["last_committed_sequence"],
            "service": service,
            "rejected": False,
        }
    except (ValueError, KeyError, StopIteration, TypeError, sqlite3.Error) as error:
        return {
            "rejected": True,
            "published_results": False,
            "reason": "journal_gap" if str(error) == "journal has a gap" else str(error),
        }
