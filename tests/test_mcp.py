from __future__ import annotations

import importlib.util
import unittest
from subprocess import CompletedProcess
from unittest.mock import patch


MCP_AVAILABLE = importlib.util.find_spec("mcp") is not None
if MCP_AVAILABLE:
    from agent_merge_queue.mcp_server import _run
else:
    _run = None


@unittest.skipUnless(MCP_AVAILABLE, "the optional MCP extra is not installed")
class McpTest(unittest.TestCase):
    def test_diagnose_can_return_failed_report_payload(self) -> None:
        completed = CompletedProcess(
            ["deploybot"],
            1,
            stdout='[{"check":"auth","status":"fail"}]\n',
            stderr="",
        )
        with patch(
            "agent_merge_queue.mcp_server.subprocess.run", return_value=completed
        ):
            value = _run("doctor", "--json", allow_nonzero=True)
        self.assertIn('"status":"fail"', value)

    def test_other_mcp_commands_still_fail_closed(self) -> None:
        completed = CompletedProcess(["deploybot"], 1, stdout="", stderr="unsafe")
        with (
            patch(
                "agent_merge_queue.mcp_server.subprocess.run", return_value=completed
            ),
            self.assertRaisesRegex(RuntimeError, "unsafe"),
        ):
            _run("drain", "--json")


if __name__ == "__main__":
    unittest.main()
