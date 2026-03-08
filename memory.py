from collections import deque
import copy


class IntentMemory:
    def __init__(self, max_size: int = 5):
        self._history = deque(maxlen=max_size)

    def add(self, intent: dict, question: str):
        self._history.append({
            "question": question,
            "intent": copy.deepcopy(intent)
        })

    def get_recent(self, n: int = 3) -> list:
        return list(self._history)[-n:]

    def merge_with_previous(self, delta: dict) -> dict:
        if not self._history:
            return delta
        merged = copy.deepcopy(self._history[-1]["intent"])
        for key, value in delta.items():
            if value is not None and value != [] and value != "":
                merged[key] = value
        return merged

    def is_followup(self, intent: dict) -> bool:
        """
        Intent-level follow-up detection only — no keyword heuristics.
        Requires same table AND same aggregation AND same metric to be considered a follow-up.
        """
        if not self._history:
            return False
        prev = self._history[-1]["intent"]

        same_table       = intent.get("fact_table") == prev.get("fact_table")
        same_aggregation = intent.get("aggregation") == prev.get("aggregation")
        same_metric      = intent.get("metric") == prev.get("metric")
        diff_group       = intent.get("group_by") != prev.get("group_by")
        diff_filter      = intent.get("filters") != prev.get("filters")

        return same_table and same_aggregation and same_metric and (diff_group or diff_filter)

    def clear(self):
        self._history.clear()

    def __len__(self):
        return len(self._history)
