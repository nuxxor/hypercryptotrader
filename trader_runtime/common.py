import argparse
import asyncio
import aiohttp
import websockets
import hmac
import hashlib
import time
import json
import logging
import os
import traceback
import re
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Tuple, Optional, Set, Sequence
from collections import deque
from decimal import Decimal, ROUND_DOWN
from dataclasses import dataclass
import random
import string
import numpy as np
import urllib
import pytz
import math
from pathlib import Path

try:
    from websockets.exceptions import InvalidStatus
except ImportError:  # websockets < 12
    InvalidStatus = websockets.exceptions.InvalidStatusCode

LOG_ROOT = Path(os.getenv("HYPERCRYPTO_LOG_DIR", "var/logs"))
LOG_ROOT.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_ROOT / 'position_manager.log')
    ]
)
logger = logging.getLogger('PositionManager')

DEFAULT_POSITION_HTTP_TOTAL_TIMEOUT = float(os.getenv('POSITION_HTTP_TOTAL_TIMEOUT', '6.0'))
DEFAULT_POSITION_HTTP_CONNECT_TIMEOUT = float(os.getenv('POSITION_HTTP_CONNECT_TIMEOUT', '2.5'))
DEFAULT_POSITION_HTTP_SOCK_CONNECT_TIMEOUT = float(os.getenv('POSITION_HTTP_SOCK_CONNECT_TIMEOUT', '2.5'))
DEFAULT_POSITION_HTTP_SOCK_READ_TIMEOUT = float(os.getenv('POSITION_HTTP_READ_TIMEOUT', '3.0'))

DEFAULT_POSITION_WS_OPEN_TIMEOUT = float(os.getenv('POSITION_WS_OPEN_TIMEOUT', '3.0'))
DEFAULT_POSITION_WS_CLOSE_TIMEOUT = float(os.getenv('POSITION_WS_CLOSE_TIMEOUT', '10.0'))
DEFAULT_POSITION_WS_PING_INTERVAL = float(os.getenv('POSITION_WS_PING_INTERVAL', '20.0'))
DEFAULT_POSITION_WS_PING_TIMEOUT = float(os.getenv('POSITION_WS_PING_TIMEOUT', '10.0'))


def _parse_retry_sequence(raw: Optional[str], default: Sequence[float]) -> Tuple[float, ...]:
    if not raw:
        return tuple(default)
    values: List[float] = []
    for chunk in raw.split(','):
        token = chunk.strip()
        if not token:
            continue
        try:
            value = float(token)
        except ValueError:
            continue
        if value < 0:
            continue
        values.append(value)
    return tuple(values) if values else tuple(default)


def _retry_delay_for_attempt(sequence: Sequence[float], tail_step: float, attempt: int) -> float:
    if attempt <= 0:
        attempt = 1
    if not sequence:
        return max(0.0, tail_step * (attempt - 1))
    index = attempt - 1
    if index < len(sequence):
        return max(0.0, sequence[index])
    tail_index = index - (len(sequence) - 1)
    return max(0.0, sequence[-1] + tail_step * tail_index)


__all__ = [
    'Any', 'Dict', 'List', 'Tuple', 'Optional', 'Set', 'Sequence',
    'Decimal', 'ROUND_DOWN', 'Path', 'datetime', 'timedelta', 'timezone', 'dataclass',
    'deque', 'asyncio', 'aiohttp', 'websockets', 'hmac', 'hashlib', 'time',
    'json', 'logging', 'os', 'traceback', 're', 'argparse', 'load_dotenv',
    'random', 'string', 'np', 'urllib', 'pytz', 'math', 'InvalidStatus', 'logger',
    'DEFAULT_POSITION_HTTP_TOTAL_TIMEOUT', 'DEFAULT_POSITION_HTTP_CONNECT_TIMEOUT',
    'DEFAULT_POSITION_HTTP_SOCK_CONNECT_TIMEOUT', 'DEFAULT_POSITION_HTTP_SOCK_READ_TIMEOUT',
    'DEFAULT_POSITION_WS_OPEN_TIMEOUT', 'DEFAULT_POSITION_WS_CLOSE_TIMEOUT',
    'DEFAULT_POSITION_WS_PING_INTERVAL', 'DEFAULT_POSITION_WS_PING_TIMEOUT',
    '_parse_retry_sequence', '_retry_delay_for_attempt',
]
