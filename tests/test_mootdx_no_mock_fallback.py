"""Tests for mootdx client mock fallback switch."""

from __future__ import annotations

from backend.realtime import mootdx_client as mc


def test_mootdx_no_mock_fallback_returns_unavailable_tick(monkeypatch) -> None:
    monkeypatch.setattr(mc, "_MOOTDX_AVAILABLE", False)
    monkeypatch.setattr(mc, "_ALLOW_MOCK_FALLBACK", False)

    c = mc.MootdxClient()
    c._client = None

    tick = c.get_tick("000001.SZ")
    assert tick.get("data_unavailable") is True
    assert tick.get("is_mock") is False
    assert tick.get("price") == 0.0


def test_mootdx_no_mock_fallback_returns_empty_bars_and_txns(monkeypatch) -> None:
    monkeypatch.setattr(mc, "_MOOTDX_AVAILABLE", False)
    monkeypatch.setattr(mc, "_ALLOW_MOCK_FALLBACK", False)

    c = mc.MootdxClient()
    c._client = None

    bars = c.get_minute_bars("000001.SZ", 120)
    txns = c.get_transactions("000001.SZ", 30)
    assert bars == []
    assert txns == []


def test_mootdx_mock_fallback_can_be_enabled(monkeypatch) -> None:
    monkeypatch.setattr(mc, "_MOOTDX_AVAILABLE", False)
    monkeypatch.setattr(mc, "_ALLOW_MOCK_FALLBACK", True)

    c = mc.MootdxClient()
    c._client = None

    tick = c.get_tick("000001.SZ")
    bars = c.get_minute_bars("000001.SZ", 5)
    txns = c.get_transactions("000001.SZ", 5)

    assert tick.get("is_mock") is True
    assert len(bars) == 5
    assert len(txns) == 5
