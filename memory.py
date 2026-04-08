"""
Intent memory and follow-up detection for Dataloom query sessions.

Owns the short-term, in-memory record of recent parsed intents within a
session. Used by the query pipeline to resolve follow-up questions and
carry forward context between turns.

Public API: IntentMemory
"""

from collections import deque
import copy


class IntentMemory:
    """Rolling buffer of recent query intents for a single session.

    Stores the last ``max_size`` (intent, question) pairs and provides
    helpers for merging a partial follow-up intent with the previous one
    and detecting whether a new intent is a follow-up at all.

    Args:
        max_size: Maximum number of intents to retain. Oldest entries are
            evicted automatically once the buffer is full. Defaults to 5.
    """

    def __init__(self, max_size: int = 5):
        self._history = deque(maxlen=max_size)

    def add(self, intent: dict, question: str) -> None:
        """Record a resolved intent alongside the original question text.

        Args:
            intent: The fully resolved intent dictionary produced by the
                intent parser.
            question: The raw user question that produced this intent.
        """
        self._history.append({
            "question": question,
            "intent": copy.deepcopy(intent)
        })

    def get_recent(self, n: int = 3) -> list:
        """Return the most recent n intents, oldest first.

        Args:
            n: Number of entries to return. Capped at the current buffer
                size. Defaults to 3.

        Returns:
            List of ``{"question": str, "intent": dict}`` dicts, ordered
            oldest to newest.
        """
        return list(self._history)[-n:]

    def merge_with_previous(self, delta: dict) -> dict:
        """Merge a partial (follow-up) intent onto the most recent full intent.

        Non-empty fields in ``delta`` override the corresponding fields in
        the previous intent. Empty values (``None``, ``[]``, ``""``) are
        treated as "unchanged" and are not applied.

        Args:
            delta: Partial intent dict produced from the follow-up question.
                Fields absent or empty in ``delta`` are inherited from the
                previous intent.

        Returns:
            A new intent dict representing the merged result. Returns
            ``delta`` unchanged if there is no prior intent in the buffer.
        """
        if not self._history:
            return delta
        merged = copy.deepcopy(self._history[-1]["intent"])
        for key, value in delta.items():
            if value is not None and value != [] and value != "":
                merged[key] = value
        return merged

    def is_followup(self, intent: dict) -> bool:
        """Determine whether ``intent`` is a follow-up to the previous query.

        Intent-level follow-up detection only — no keyword heuristics.
        Requires same table AND same aggregation AND same metric to be considered a follow-up.

        A follow-up is defined as: same fact table, same aggregation, and
        same metric as the previous intent, but with a different group-by
        or filter. This catches "now break that down by region" style
        questions while ignoring unrelated queries that happen to share
        some fields.

        Args:
            intent: The newly parsed intent dict to evaluate.

        Returns:
            ``True`` if the intent looks like a follow-up to the most
            recent recorded intent; ``False`` if the buffer is empty or
            the criteria are not met.
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

    def clear(self) -> None:
        """Discard all recorded intents from the buffer."""
        self._history.clear()

    def __len__(self) -> int:
        return len(self._history)
