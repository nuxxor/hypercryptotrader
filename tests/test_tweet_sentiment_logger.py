import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tweet_sentiment_logger import TweetSentimentLogger


def _make_logger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> TweetSentimentLogger:
    cmc_path = tmp_path / "cmc.json"
    cmc_path.write_text("{}", encoding="utf-8")
    monkeypatch.setenv("TREE_NEWS_TARGET_TRACE_LOG", str(tmp_path / "trace.log"))
    return TweetSentimentLogger(
        cmc_path=cmc_path,
        log_path=tmp_path / "out.log",
        enable_sentiment=False,
    )


def test_handle_prefers_screen_name_and_falls_back_to_title(tmp_path, monkeypatch):
    logger = _make_logger(tmp_path, monkeypatch)
    payload = {
        "author": {"name": "vitalik.eth"},  # Display name should not be treated as handle
        "title": "vitalik.eth (@VitalikButerin): Test tweet content",
        "url": "https://twitter.com/VitalikButerin/status/123",
    }

    handle = logger._extract_handle(payload)

    assert handle == "vitalikbuterin"


def test_handle_uses_info_when_author_name_invalid(tmp_path, monkeypatch):
    logger = _make_logger(tmp_path, monkeypatch)
    payload = {
        "author": {"name": "The Block"},
        "info": {"screenName": "TheBlock__", "twitterUsername": "TheBlock__"},
    }

    handle = logger._extract_handle(payload)

    assert handle == "theblock__"
