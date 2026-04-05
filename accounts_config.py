"""Account configuration helpers for primary and optional extended accounts.

This module consolidates credentials defined across `.env` and optional
`.env2` files so trading components can operate from a normalized account
roster when needed. In the solo public setup, the primary account from
`.env` is enough; optional extra accounts are only loaded when explicitly
configured.

Usage examples:
    from accounts_config import load_binance_accounts
    accounts = load_binance_accounts()
    for account in accounts:
        print(account.name, account.binance.api_key)

The loader never logs or prints secrets and keeps environment precedence:
values exported in the current process win over `.env2` entries.
"""

from __future__ import annotations

import functools
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, Optional

from dotenv import dotenv_values


@dataclass(frozen=True)
class ExchangeCredentials:
    """API credential pair for a specific exchange."""

    api_key: str
    api_secret: str


@dataclass(frozen=True)
class ExchangeNetwork:
    """Optional network routing settings for an exchange account."""

    local_ip: Optional[str] = None
    proxy_url: Optional[str] = None


@dataclass(frozen=True)
class AccountDefinition:
    """Normalized credential bundle for a trader account."""

    name: str  # lowercase slug used by bots (e.g., "primary", "alpygood")
    label: str  # uppercase label used by monitoring/logs (e.g., "PRIMARY")
    role: str  # "primary" or optional extended account
    binance: Optional[ExchangeCredentials] = None
    gate: Optional[ExchangeCredentials] = None
    mexc: Optional[ExchangeCredentials] = None
    network: Dict[str, ExchangeNetwork] = field(default_factory=dict)


def load_account_definitions(
    include_primary: bool = True,
    env2_path: str | Path = ".env2",
    network_config_path: str | Path = "account_network.json",
) -> List[AccountDefinition]:
    """Return all known accounts with any exchange credentials.

    Parameters
    ----------
    include_primary:
        Whether to include the primary account drawn from BINANCE_/GATE_/MEXC_ env vars.
    env2_path:
        Optional secondary env file that stores follower credentials.
    """

    env2_file = Path(env2_path).expanduser()
    env2_values: Mapping[str, str] = _load_env_file(env2_file)
    network_map = _load_network_config(Path(network_config_path))

    accounts: List[AccountDefinition] = []

    if include_primary:
        primary_account = _build_primary_account(env2_values)
        if primary_account is not None:
            accounts.append(primary_account)

    follower_prefixes = _discover_follower_prefixes(env2_values)

    for slug, source_prefix in sorted(follower_prefixes.items()):
        binance = _build_exchange_credentials(source_prefix, "BINANCE", env2_values)
        gate = _build_exchange_credentials(source_prefix, "GATE", env2_values)
        mexc = _build_exchange_credentials(source_prefix, "MEXC", env2_values)

        if not any((binance, gate, mexc)):
            continue

        network = _build_network_config(slug, network_map)

        accounts.append(
            AccountDefinition(
                name=slug,
                label=slug.upper(),
                role="follower",
                binance=binance,
                gate=gate,
                mexc=mexc,
                network=network,
            )
        )

    return accounts


def load_binance_accounts(
    include_primary: bool = True,
    env2_path: str | Path = ".env2",
    network_config_path: str | Path = "account_network.json",
) -> List[AccountDefinition]:
    """Convenience helper that filters only accounts with Binance creds."""

    return [
        account
        for account in load_account_definitions(
            include_primary=include_primary,
            env2_path=env2_path,
            network_config_path=network_config_path,
        )
        if account.binance is not None
    ]


@functools.lru_cache(maxsize=1)
def _load_env_file(path: Path) -> Mapping[str, str]:
    if path.exists():
        values = {k: v for k, v in dotenv_values(path).items() if v}
        return values
    return {}


@functools.lru_cache(maxsize=1)
def _load_network_config(path: Path) -> Mapping[str, Mapping[str, str]]:
    if path.exists():
        try:
            import json

            with path.open("r", encoding="utf-8") as handle:
                data = json.load(handle)
                if isinstance(data, dict):
                    return data
        except Exception:
            pass
    return {}


def _resolve_value(key: str, env2_values: Mapping[str, str]) -> Optional[str]:
    val = os.getenv(key)
    if val and val.strip():
        return val.strip()
    val = env2_values.get(key)
    if val and val.strip():
        return val.strip()
    key_upper = key.upper()
    for candidate, value in env2_values.items():
        if candidate.upper() == key_upper and value and value.strip():
            return value.strip()
    return None


def _build_primary_account(env2_values: Mapping[str, str]) -> Optional[AccountDefinition]:
    binance = _build_exchange_credentials("", "BINANCE", env2_values, use_global=True)
    gate = _build_exchange_credentials("", "GATE", env2_values, use_global=True)
    mexc = _build_exchange_credentials("", "MEXC", env2_values, use_global=True)

    if not any((binance, gate, mexc)):
        return None

    return AccountDefinition(
        name="primary",
        label="PRIMARY",
        role="primary",
        binance=binance,
        gate=gate,
        mexc=mexc,
        network={},
    )


def _discover_follower_prefixes(env2_values: Mapping[str, str]) -> Dict[str, str]:
    suffixes = (
        "_BINANCE_API_KEY",
        "_GATE_API_KEY",
        "_MEXC_API_KEY",
    )
    reserved_keys = {"BINANCE_API_KEY", "GATE_API_KEY", "MEXC_API_KEY"}
    reserved_prefixes = {"", "PRIMARY", "BINANCE", "GATE", "MEXC"}

    prefixes: Dict[str, str] = {}

    def _maybe_add_prefix(source_keys: Iterable[str]):
        for key in source_keys:
            if key in reserved_keys:
                continue
            for suffix in suffixes:
                if key.endswith(suffix):
                    prefix = key[: -len(suffix)]
                    if prefix.upper() in reserved_prefixes:
                        break
                    slug = prefix.lower()
                    prefixes.setdefault(slug, prefix)
                    break

    _maybe_add_prefix(env2_values.keys())
    _maybe_add_prefix(os.environ.keys())

    return prefixes


def _build_exchange_credentials(
    prefix: str,
    exchange: str,
    env2_values: Mapping[str, str],
    use_global: bool = False,
) -> Optional[ExchangeCredentials]:
    """Construct credentials for the given exchange and prefix."""

    if use_global:
        key_name = f"{exchange}_API_KEY"
        secret_name = f"{exchange}_API_SECRET"
        api_key = _resolve_value(key_name, env2_values)
        api_secret = _resolve_value(secret_name, env2_values)
        if api_key and api_secret:
            return ExchangeCredentials(api_key=api_key, api_secret=api_secret)
        return None

    candidates: list[str] = []
    for candidate in (prefix, prefix.lower(), prefix.upper(), prefix.capitalize()):
        if candidate and candidate not in candidates:
            candidates.append(candidate)

    for candidate in candidates:
        key_name = f"{candidate}_{exchange}_API_KEY"
        secret_name = f"{candidate}_{exchange}_API_SECRET"
        api_key = _resolve_value(key_name, env2_values)
        api_secret = _resolve_value(secret_name, env2_values)
        if api_key and api_secret:
            return ExchangeCredentials(api_key=api_key, api_secret=api_secret)

    return None


def _build_network_config(
    slug: str,
    network_map: Mapping[str, Mapping[str, Mapping[str, str]]],
) -> Dict[str, ExchangeNetwork]:
    exchange_map = network_map.get(slug) or network_map.get(slug.upper())
    if not isinstance(exchange_map, dict):
        return {}

    result: Dict[str, ExchangeNetwork] = {}
    for exchange, config in exchange_map.items():
        if not isinstance(config, dict):
            continue

        local_ip = (config.get("local_ip") or config.get("localIp") or "").strip() or None
        if not local_ip:
            public_ip = (
                config.get("public_ip")
                or config.get("publicIp")
                or config.get("publicIP")
                or ""
            )
            local_ip = str(public_ip).strip() or None
        if not local_ip:
            public_ips = config.get("public_ips") or config.get("publicIps")
            if isinstance(public_ips, (list, tuple)):
                for candidate in public_ips:
                    candidate_str = str(candidate).strip()
                    if candidate_str:
                        local_ip = candidate_str
                        break
        proxy_url = (config.get("proxy") or config.get("proxy_url") or "").strip() or None

        if local_ip or proxy_url:
            result[exchange.lower()] = ExchangeNetwork(local_ip=local_ip, proxy_url=proxy_url)

    return result
