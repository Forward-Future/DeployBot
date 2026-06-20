from __future__ import annotations

import unittest

from agent_merge_queue.config import ReviewProviderConfig
from agent_merge_queue.reviews import evaluate_reviews


HEAD = "a" * 40


class ReviewProviderTest(unittest.TestCase):
    def test_no_provider_means_checks_only(self) -> None:
        self.assertEqual(
            evaluate_reviews(
                [],
                head_sha=HEAD,
                checks={"CI": "passed"},
                comments=[],
                reviews=[],
                threads=[],
            ),
            (),
        )

    def test_human_approval_is_bound_to_exact_head(self) -> None:
        policy = ReviewProviderConfig(
            kind="github-approvals",
            name="Human",
            allowed_reviewers=("reviewer",),
            minimum_approvals=1,
        )
        stale = evaluate_reviews(
            [policy],
            head_sha=HEAD,
            checks={},
            comments=[],
            reviews=[
                {
                    "state": "APPROVED",
                    "commit_id": "b" * 40,
                    "user": {"login": "reviewer"},
                }
            ],
            threads=[],
        )
        current = evaluate_reviews(
            [policy],
            head_sha=HEAD,
            checks={},
            comments=[],
            reviews=[
                {
                    "state": "APPROVED",
                    "commit_id": HEAD,
                    "user": {"login": "reviewer"},
                }
            ],
            threads=[],
        )

        self.assertEqual(stale[0].state, "waiting")
        self.assertEqual(current[0].state, "passed")

    def test_latest_review_state_wins_for_each_human(self) -> None:
        policy = ReviewProviderConfig(
            kind="github-approvals",
            name="Human",
            allowed_reviewers=("reviewer",),
            minimum_approvals=1,
        )
        verdict = evaluate_reviews(
            [policy],
            head_sha=HEAD,
            checks={},
            comments=[],
            reviews=[
                {
                    "state": "APPROVED",
                    "commit_id": HEAD,
                    "submitted_at": "2026-06-20T00:00:00Z",
                    "user": {"login": "reviewer"},
                },
                {
                    "state": "CHANGES_REQUESTED",
                    "commit_id": HEAD,
                    "submitted_at": "2026-06-20T00:01:00Z",
                    "user": {"login": "reviewer"},
                },
            ],
            threads=[],
        )[0]

        self.assertEqual(verdict.state, "waiting")

    def test_unlisted_approval_does_not_count(self) -> None:
        policy = ReviewProviderConfig(
            kind="github-approvals",
            name="Human",
            allowed_reviewers=("trusted-reviewer",),
            minimum_approvals=1,
        )
        verdict = evaluate_reviews(
            [policy],
            head_sha=HEAD,
            checks={},
            comments=[],
            reviews=[
                {
                    "state": "APPROVED",
                    "commit_id": HEAD,
                    "user": {"login": "drive-by-reviewer"},
                }
            ],
            threads=[],
        )[0]

        self.assertEqual(verdict.state, "waiting")

    def test_bot_score_and_threads_fail_closed(self) -> None:
        policy = ReviewProviderConfig(
            kind="bot",
            name="Review bot",
            login="review-bot",
            check_name="Review Bot",
            minimum_score=4,
            score_pattern=r"Score:\s*(\d)",
            require_formal_review=True,
            require_resolved_threads=True,
        )
        verdict = evaluate_reviews(
            [policy],
            head_sha=HEAD,
            checks={"Review Bot": "passed"},
            comments=[
                {
                    "created_at": "2026-06-20T00:00:00Z",
                    "user": {"login": "review-bot[bot]"},
                    "body": f"Score: 5\ncommit {HEAD}",
                }
            ],
            reviews=[
                {
                    "commit_id": HEAD,
                    "user": {"login": "review-bot[bot]"},
                }
            ],
            threads=[
                {
                    "isResolved": False,
                    "isOutdated": False,
                    "comments": {
                        "nodes": [{"author": {"login": "review-bot[bot]"}}]
                    },
                }
            ],
        )[0]

        self.assertEqual(verdict.state, "blocked")
        self.assertEqual(verdict.score, 5)
        self.assertIn("unresolved", verdict.reasons[0])

    def test_generic_check_provider(self) -> None:
        policy = ReviewProviderConfig(
            kind="check", name="Agent review", check_name="Agent Review"
        )
        verdict = evaluate_reviews(
            [policy],
            head_sha=HEAD,
            checks={"Agent Review": "SUCCESS".lower().replace("success", "passed")},
            comments=[],
            reviews=[],
            threads=[],
        )[0]

        self.assertEqual(verdict.state, "passed")

    def test_non_numeric_bot_score_fails_closed(self) -> None:
        policy = ReviewProviderConfig(
            kind="bot",
            name="Review bot",
            login="review-bot",
            minimum_score=4,
            score_pattern=r"Score:\s*(\S+)",
        )

        verdict = evaluate_reviews(
            [policy],
            head_sha=HEAD,
            checks={},
            comments=[
                {
                    "created_at": "2026-06-20T00:00:00Z",
                    "user": {"login": "review-bot"},
                    "body": f"Score: N/A\ncommit {HEAD}",
                }
            ],
            reviews=[],
            threads=[],
        )[0]

        self.assertEqual(verdict.state, "waiting")
        self.assertIsNone(verdict.score)

    def test_missing_optional_bot_score_capture_fails_closed(self) -> None:
        policy = ReviewProviderConfig(
            kind="bot",
            name="Review bot",
            login="review-bot",
            minimum_score=4,
            score_pattern=r"Score:(?:\s*(\d+))?",
        )

        verdict = evaluate_reviews(
            [policy],
            head_sha=HEAD,
            checks={},
            comments=[
                {
                    "created_at": "2026-06-20T00:00:00Z",
                    "user": {"login": "review-bot"},
                    "body": f"Score:\ncommit {HEAD}",
                }
            ],
            reviews=[],
            threads=[],
        )[0]

        self.assertEqual(verdict.state, "waiting")
        self.assertIsNone(verdict.score)


if __name__ == "__main__":
    unittest.main()
