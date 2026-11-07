"""Independent recovery oracle: reduce complete history from genesis, ignoring snapshots."""


def expected(raw):
    if raw["scenario"] == "invalid":
        return {"rejected": True, "published_results": False, "reason": "journal_gap"}
    # Reverse search for each ticket's last operation avoids sharing the executor's replay reducer.
    relevant = [t for t in raw["history"] if t["sequence"] <= raw["target_sequence"]]
    latest = {}
    for transaction in reversed(relevant):
        for op in reversed(transaction["operations"]):
            if op["id"] not in latest:
                latest[op["id"]] = op
    rows = [[key, op["subject"], op["status"]] for key, op in sorted(latest.items()) if op["op"] == "put"]
    return {
        "tickets": rows,
        "integrity": "ok",
        "cutoff": raw["target_sequence"],
        "service": {"health": True, "ticket_count": len(rows)},
        "rejected": False,
    }
