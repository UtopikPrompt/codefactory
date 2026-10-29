"""Regression coverage for the read-only, allowlisted coordination projection."""

import json

import pytest
from task_api import coordination_summary


def test_coordination_never_exposes_plan_or_raw_feedback():
    summary = coordination_summary({
        "plan": "private source and instructions",
        "retry_feedback": json.dumps({"stage": "judge", "attempt": 1, "review": "secret",
                                     "reason": "secret", "review_topics": ["tests", "secret", "scope", "tests"]}),
    })
    assert summary == {"plan_recorded": True, "latest_feedback": {
        "stage": "judge", "attempt": 1, "review_topics": ["tests", "scope"],
    }}
    assert "private" not in json.dumps(summary) and "secret" not in json.dumps(summary)


@pytest.mark.parametrize("feedback", [
    None, "", "invalid json", "[]", "true", '{"stage":"secret","attempt":1}',
    '{"stage":"judge","attempt":true}', '{"stage":"judge","attempt":0}',
    '{"stage":"judge","attempt":9}',
])
def test_coordination_handles_missing_or_invalid_feedback(feedback):
    assert coordination_summary({"plan": None, "retry_feedback": feedback}) == {
        "plan_recorded": False, "latest_feedback": None,
    }


@pytest.mark.parametrize("stage", ["supervisor", "worker", "judge"])
def test_coordination_topics_require_an_allowlisted_list(stage):
    assert coordination_summary({"plan": None, "retry_feedback": json.dumps({
        "stage": stage, "attempt": 8, "review_topics": "tests security secret",
    })})["latest_feedback"] == {"stage": stage, "attempt": 8, "review_topics": []}
