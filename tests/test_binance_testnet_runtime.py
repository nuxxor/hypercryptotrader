import asyncio
import base64
import sys
from pathlib import Path

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import ed25519

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import trader  # noqa: E402


def test_binance_testnet_mode_uses_network_paths():
    api = trader.BinanceAPI(
        "key",
        "secret",
        session=None,
        test_mode=True,
        account_name="PRIMARY",
        use_testnet=True,
    )

    observed_calls = []

    async def fake_send_request(method, endpoint, params=None, signed=False):
        observed_calls.append((method, endpoint, params, signed))
        return {"price": "123.45"}

    api._send_request = fake_send_request

    price = asyncio.run(api.get_price("BTCUSDT"))

    assert price == pytest.approx(123.45)
    assert observed_calls == [
        ("GET", "/api/v3/ticker/price", {"symbol": "BTCUSDT"}, False)
    ]
    assert api.base_url == "https://testnet.binance.vision"
    assert api.stream_url_base == "wss://stream.testnet.binance.vision"
    assert api.simulated_mode is False


def test_position_manager_accepts_binance_only_mode(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("BINANCE_API_KEY", "binance-key")
    monkeypatch.setenv("BINANCE_API_SECRET", "binance-secret")
    monkeypatch.delenv("GATE_API_KEY", raising=False)
    monkeypatch.delenv("GATE_API_SECRET", raising=False)
    monkeypatch.delenv("MEXC_API_KEY", raising=False)
    monkeypatch.delenv("MEXC_API_SECRET", raising=False)

    manager = trader.PositionManager(
        test_mode=True,
        enabled_exchanges=["binance"],
        binance_use_testnet=True,
        position_ws_enabled=False,
    )

    assert manager.enabled_exchanges == {"binance"}
    assert manager.binance_use_testnet is True
    assert manager.position_ws_enabled is False
    assert manager.gate_api is None
    assert manager.mexc_api is None


def test_binance_testnet_ed25519_signature(monkeypatch, tmp_path):
    private_key = ed25519.Ed25519PrivateKey.generate()
    private_path = tmp_path / "test_ed25519_private.pem"
    private_path.write_bytes(
        private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    )

    monkeypatch.setenv("BINANCE_TESTNET_KEY_TYPE", "ed25519")
    monkeypatch.setenv("BINANCE_TESTNET_PRIVATE_KEY_PATH", str(private_path))

    api = trader.BinanceAPI(
        "api-key",
        "",
        session=None,
        test_mode=False,
        account_name="PRIMARY",
        use_testnet=True,
    )

    params = {"symbol": "BTCUSDT", "timestamp": 123456789}
    signature = asyncio.run(api._generate_signature(params))

    payload = b"symbol=BTCUSDT&timestamp=123456789"
    private_key.public_key().verify(base64.b64decode(signature), payload)
