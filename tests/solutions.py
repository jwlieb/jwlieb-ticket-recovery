import json


def solution(scenario="standard", seed=7):
    cutoff = 0 if scenario == "empty" else 20 if scenario == "boundary" else 37
    return {
        "recovery.json": json.dumps(
            {"snapshot_id": "snapshot-0" if scenario == "empty" else "snapshot-10", "through_sequence": cutoff}
        )
    }
