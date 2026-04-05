#!/usr/bin/env python3
# sentiment_cli.py — Bedrock Claude backend for tweet sentiment analysis
# Targets=default (auto cashtag/paren), Targeted multi-head voting (stance+fav+unfav+optimistic),
# General multi-head, SHILL detection, verbose JSON, graceful shutdown, retry, rate-limit, cache.

import os
import sys
import re
import time
import signal
import argparse
import json as _json
import runpy
import hashlib
import hmac
from datetime import datetime
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional
from collections import OrderedDict
from pathlib import Path
from urllib import request as urlrequest, error as urlerror, parse as urlparse

from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()
_running = True


# ============================== Graceful IO ==============================

def _handle_signal(signum, frame):
    global _running
    if _running:
        _running = False
        console.print("\n[bold yellow]Shutdown requested. Exiting…[/]")
    else:
        os._exit(0)

signal.signal(signal.SIGINT, _handle_signal)
signal.signal(signal.SIGTERM, _handle_signal)

def read_line_interruptible(prompt: str = "Tweet: ") -> str:
    """Read a line that can be interrupted instantly with Ctrl+C (POSIX/Windows)."""
    try:
        if os.name == "nt":
            import msvcrt
            sys.stdout.write(prompt); sys.stdout.flush()
            buf = []
            while _running:
                if msvcrt.kbhit():
                    ch = msvcrt.getwch()
                    if ch == '\x03':
                        raise KeyboardInterrupt
                    if ch in ('\r', '\n'):
                        sys.stdout.write("\n"); sys.stdout.flush()
                        return "".join(buf)
                    if ch == '\x08':
                        if buf:
                            buf.pop()
                            sys.stdout.write('\b \b'); sys.stdout.flush()
                        continue
                    buf.append(ch); sys.stdout.write(ch); sys.stdout.flush()
                else:
                    time.sleep(0.1)
            return ""
        else:
            import select
            sys.stdout.write(prompt); sys.stdout.flush()
            while _running:
                r, _, _ = select.select([sys.stdin], [], [], 0.1)
                if r:
                    line = sys.stdin.readline()
                    return line.rstrip("\n")
            return ""
    except KeyboardInterrupt:
        _handle_signal(signal.SIGINT, None)
        return ""


# ============================== Targets Extraction ==============================

CASHTAG_RE = re.compile(r"\$([A-Za-z][A-Za-z0-9\-]{1,14})\b")
PAREN_RE   = re.compile(r"\(([A-Za-z][A-Za-z0-9\-]{1,14})\)")

def extract_targets_auto(tweet: str, max_targets: int = 10) -> List[str]:
    cands = set(CASHTAG_RE.findall(tweet))
    cands.update(PAREN_RE.findall(tweet))
    toks = sorted({t.upper() for t in cands if 2 <= len(t) <= 15})
    return toks[:max_targets]


# ============================== Prompts ==============================

STANCE_RULES = (
    "You are a strict targeted sentiment classifier for tweets.\n"
    "Output exactly one token from {POS, NEG, NEU}.\n"
    "Classify the author's stance towards the given TARGET only.\n"
    "Answer with a single token only."
)

FAVORABLE_RULES = (
    "You are a strict targeted sentiment checker.\n"
    "Answer with exactly one token from {YES, NO, UNCERTAIN}.\n"
    "Question: Is the author's stance favorable towards the given TARGET?"
)

UNFAVORABLE_RULES = (
    "You are a strict targeted sentiment checker.\n"
    "Answer with exactly one token from {YES, NO, UNCERTAIN}.\n"
    "Question: Is the author's stance unfavorable (criticism/doubt/pessimism) towards the given TARGET?"
)

OPTIMISTIC_RULES = (
    "You are a strict targeted sentiment checker.\n"
    "Answer with exactly one token from {YES, NO, UNCERTAIN}.\n"
    "Question: Is the author optimistic about the TARGET's future performance or resilience?"
)

# ============================== Overall Prompts (multi-head) ==============================
GENERAL_STANCE_RULES = (
    "You are a strict overall sentiment classifier.\n"
    "Output exactly one token from {POS, NEG, NEU}.\n"
    "Judge the author's overall stance.\n"
    "Answer with a single token only."
)
GENERAL_FAV_RULES = (
    "You are a strict sentiment checker.\n"
    "Answer with exactly one token from {YES, NO, UNCERTAIN}.\n"
    "Question: Is the author's stance favorable overall?"
)
GENERAL_UNFAV_RULES = (
    "You are a strict sentiment checker.\n"
    "Answer with exactly one token from {YES, NO, UNCERTAIN}.\n"
    "Question: Is the author's stance unfavorable (criticism/doubt/pessimism) overall?"
)

# ============================== SHILL Detection (Sophisticated) ==============================
SHILL_DETECTION_RULES = (
    "You detect if an influential crypto figure is ENDORSING a specific coin/token.\n"
    "These are sophisticated investors - they don't say 'buy this'. Look for SUBTLE signals.\n\n"
    "Output: SHILL or NO\n\n"
    "SHILL signals (any ONE is enough):\n"
    "- Personal position: bought, holding, accumulated, 'I own', 'in my portfolio'\n"
    "- Implicit endorsement: 'interesting project', 'worth watching', 'undervalued'\n"
    "- Technical praise: 'elegant solution', 'solves X problem', 'innovative approach'\n"
    "- Comparative advantage: 'X is insurance against Y', 'X does what Y cannot'\n"
    "- Future conviction: 'will be important', 'has potential', 'sleeping giant'\n"
    "- Subtle recommendation: 'DYOR', 'look into this', 'pay attention to'\n"
    "- Strategic framing: positioning a coin as hedge, diversification, or necessity\n\n"
    "NO signals:\n"
    "- Pure news/facts without opinion ('X announced Y')\n"
    "- Neutral technical discussion without endorsement\n"
    "- Questions or speculation without conviction\n"
    "- Criticism or warnings about the coin\n"
    "- General market commentary not endorsing specific coin\n\n"
    "Remember: These are VIPs. Even mild positive framing can move markets.\n"
    "When in doubt between SHILL and NO, choose SHILL - false negatives are worse."
)


# ============================== Small TTL Cache ==============================

class TTLCache:
    def __init__(self, max_items: int = 4096, ttl_seconds: int = 300):
        self.max_items = max_items
        self.ttl = ttl_seconds
        self.store: OrderedDict[str, Tuple[float, dict]] = OrderedDict()

    def _prune(self):
        now = time.time()
        keys = list(self.store.keys())
        for k in keys:
            ts, _ = self.store[k]
            if now - ts > self.ttl:
                self.store.pop(k, None)
        while len(self.store) > self.max_items:
            self.store.popitem(last=False)

    def get(self, key: str) -> Optional[dict]:
        self._prune()
        item = self.store.get(key)
        if not item:
            return None
        ts, val = item
        if time.time() - ts > self.ttl:
            self.store.pop(key, None)
            return None
        self.store.move_to_end(key)
        return val

    def set(self, key: str, val: dict):
        self._prune()
        self.store[key] = (time.time(), val)
        self.store.move_to_end(key)


# ============================== Bedrock Claude Classifier ==============================

@dataclass
class BedrockClaudeConfig:
    model_id: str
    region: str = "ap-northeast-1"
    aws_access_key: Optional[str] = None
    aws_secret_key: Optional[str] = None
    aws_session_token: Optional[str] = None
    api_key: Optional[str] = None
    bearer_token: Optional[str] = None
    temp: float = 0.0
    top_p: float = 0.9
    max_output_tokens: int = 32
    timeout: float = 15.0
    retries: int = 2
    retry_backoff: float = 0.25
    min_interval: float = 0.08


class BedrockClaudeClassifier:
    """AWS Bedrock Claude backend for sentiment classification."""

    def __init__(self, cfg: BedrockClaudeConfig, set_timeout: Optional[float] = None):  # noqa: ARG002
        if not ((cfg.aws_access_key and cfg.aws_secret_key) or cfg.api_key or cfg.bearer_token):
            raise SystemExit(
                "Bedrock backend requires either AWS credentials or BEDROCK_API_KEY / BEDROCK_BEARER_TOKEN."
            )

        self.cfg = cfg
        self.host = f"bedrock-runtime.{cfg.region}.amazonaws.com"
        encoded_model = urlparse.quote(cfg.model_id, safe=":")  # preserve colon version suffix
        self.path = f"/model/{encoded_model}/invoke"
        self.endpoint = f"https://{self.host}{self.path}"
        self._last_call = 0.0
        self.cache = TTLCache(max_items=4096, ttl_seconds=300)
        console.print(f"[bold green]Bedrock Claude[/] {cfg.model_id} ({cfg.region})")

    @staticmethod
    def _normalize_token(text: str, allowed: Tuple[str, ...]) -> str:
        up = str(text).strip().upper()
        for piece in up.replace(".", " ").replace(",", " ").split():
            if piece in allowed:
                return piece
        for line in up.splitlines():
            w = line.strip().split()
            if w and w[0] in allowed:
                return w[0]
        return allowed[-1]

    def _rate_limit(self):
        elapsed = time.time() - self._last_call
        wait_for = self.cfg.min_interval - elapsed
        if wait_for > 0:
            time.sleep(wait_for)

    @staticmethod
    def _get_signature_key(key: str, date_stamp: str, region: str, service: str) -> bytes:
        def _sign(k, msg):
            return hmac.new(k, msg.encode("utf-8"), hashlib.sha256).digest()

        k_date = _sign(("AWS4" + key).encode("utf-8"), date_stamp)
        k_region = _sign(k_date, region)
        k_service = _sign(k_region, service)
        return _sign(k_service, "aws4_request")

    def _build_headers(self, body: str) -> Dict[str, str]:
        now = datetime.utcnow()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        payload_hash = hashlib.sha256(body.encode("utf-8")).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "Host": self.host,
            "X-Amz-Content-Sha256": payload_hash,
            "X-Amz-Date": amz_date,
        }
        if self.cfg.api_key:
            headers["x-amz-api-key"] = self.cfg.api_key
        using_sigv4 = bool(self.cfg.aws_access_key and self.cfg.aws_secret_key)
        if not using_sigv4:
            if self.cfg.bearer_token:
                headers["Authorization"] = f"Bearer {self.cfg.bearer_token}"
            return headers

        canonical_headers = (
            "content-type:application/json\n"
            f"host:{self.host}\n"
            f"x-amz-content-sha256:{payload_hash}\n"
            f"x-amz-date:{amz_date}\n"
        )
        signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
        if self.cfg.aws_session_token:
            canonical_headers += f"x-amz-security-token:{self.cfg.aws_session_token}\n"
            signed_headers += ";x-amz-security-token"
        canonical_request = (
            "POST\n"
            f"{self.path}\n"
            "\n"
            f"{canonical_headers}\n"
            f"{signed_headers}\n"
            f"{payload_hash}"
        )
        algorithm = "AWS4-HMAC-SHA256"
        credential_scope = f"{date_stamp}/{self.cfg.region}/bedrock/aws4_request"
        string_to_sign = (
            f"{algorithm}\n{amz_date}\n{credential_scope}\n"
            f"{hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()}"
        )
        signing_key = self._get_signature_key(
            self.cfg.aws_secret_key,
            date_stamp,
            self.cfg.region,
            "bedrock",
        )
        signature = hmac.new(signing_key, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()
        authorization = (
            f"{algorithm} Credential={self.cfg.aws_access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, Signature={signature}"
        )
        headers["Authorization"] = authorization
        if self.cfg.aws_session_token:
            headers["X-Amz-Security-Token"] = self.cfg.aws_session_token
        if self.cfg.bearer_token:
            headers["X-Amz-Bearer-Token"] = self.cfg.bearer_token
        return headers

    @staticmethod
    def _extract_text(content: List[Dict]) -> str:
        pieces: List[str] = []
        for block in content or []:
            if isinstance(block, dict) and block.get("type") == "text":
                text = block.get("text")
                if text:
                    pieces.append(str(text))
        return " ".join(pieces).strip()

    def _ask(
        self,
        sys_prompt: str,
        user_prompt: str,
        allowed: Tuple[str, ...],
        cache_key: Optional[str] = None,
    ) -> str:
        if cache_key:
            cached = self.cache.get(cache_key)
            if cached is not None:
                return cached.get("token", allowed[-1])

        last_err = None
        for attempt in range(self.cfg.retries + 1):
            try:
                self._rate_limit()
                payload = {
                    "anthropic_version": "bedrock-2023-05-31",
                    "system": sys_prompt,
                    "messages": [
                        {
                            "role": "user",
                            "content": [{"type": "text", "text": user_prompt}],
                        }
                    ],
                    "max_tokens": self.cfg.max_output_tokens,
                    "temperature": self.cfg.temp,
                    "top_p": self.cfg.top_p,
                }
                body = _json.dumps(payload)
                headers = self._build_headers(body)
                req = urlrequest.Request(
                    self.endpoint,
                    data=body.encode("utf-8"),
                    method="POST",
                    headers=headers,
                )
                with urlrequest.urlopen(req, timeout=self.cfg.timeout) as resp:
                    data = resp.read().decode("utf-8")
                response_json = _json.loads(data)
                text = self._extract_text(response_json.get("content", []))
                token = self._normalize_token(text, allowed)
                self._last_call = time.time()
                if cache_key:
                    self.cache.set(cache_key, {"token": token})
                return token
            except urlerror.HTTPError as e:
                err_body = e.read().decode("utf-8", errors="ignore")
                last_err = f"{e.code}: {err_body}"
                time.sleep(self.cfg.retry_backoff * (attempt + 1))
            except Exception as e:
                last_err = e
                time.sleep(self.cfg.retry_backoff * (attempt + 1))
        raise RuntimeError(f"Bedrock Claude request failed: {last_err}")

    def classify_general_multihead(self, tweet: str) -> Dict[str, str]:
        s = self._ask(
            GENERAL_STANCE_RULES,
            f"Tweet:\n{tweet}\nAnswer:",
            ("POS", "NEG", "NEU"),
            cache_key=f"gen:stance:{hash(tweet)}",
        )
        fv = self._ask(
            GENERAL_FAV_RULES,
            f"Tweet:\n{tweet}\nAnswer:",
            ("YES", "NO", "UNCERTAIN"),
            cache_key=f"gen:fav:{hash(tweet)}",
        )
        uf = self._ask(
            GENERAL_UNFAV_RULES,
            f"Tweet:\n{tweet}\nAnswer:",
            ("YES", "NO", "UNCERTAIN"),
            cache_key=f"gen:unfav:{hash(tweet)}",
        )
        if s == "POS" or fv == "YES":
            final = "POS"
        elif s == "NEG" or uf == "YES":
            final = "NEG"
        else:
            final = "NEU"
        return {"stance": s, "favorable": fv, "unfavorable": uf, "final": final}

    def classify_target_multihead(self, tweet: str, target: str) -> Dict[str, str]:
        base = f"Detected targets: {target}\nTweet:\n{tweet}\nTARGET: {target}\nAnswer:"
        s = self._ask(
            STANCE_RULES, base, ("POS", "NEG", "NEU"), cache_key=f"t:stance:{hash((tweet, target))}"
        )
        fv = self._ask(
            FAVORABLE_RULES,
            base,
            ("YES", "NO", "UNCERTAIN"),
            cache_key=f"t:fav:{hash((tweet, target))}",
        )
        uf = self._ask(
            UNFAVORABLE_RULES,
            base,
            ("YES", "NO", "UNCERTAIN"),
            cache_key=f"t:unfav:{hash((tweet, target))}",
        )
        op = self._ask(
            OPTIMISTIC_RULES,
            base,
            ("YES", "NO", "UNCERTAIN"),
            cache_key=f"t:opt:{hash((tweet, target))}",
        )

        if s == "POS" or fv == "YES" or op == "YES":
            final = "POS"
        elif s == "NEG" or uf == "YES":
            final = "NEG"
        else:
            final = "NEU"
        return {"stance": s, "favorable": fv, "unfavorable": uf, "optimistic": op, "final": final}

    def classify_targets(self, tweet: str, targets: List[str]) -> Dict[str, Dict[str, str]]:
        return {t: self.classify_target_multihead(tweet, t) for t in targets}

    def classify_shill(self, tweet: str, target: str) -> Dict[str, str]:
        """Detect if the tweet is shilling a specific target coin."""
        prompt = f"Coin/Token: {target}\nTweet:\n{tweet}\nAnswer:"
        result = self._ask(
            SHILL_DETECTION_RULES,
            prompt,
            ("SHILL", "NO"),
            cache_key=f"shill:{hash((tweet, target))}"
        )
        return {"target": target, "shill": result}

    def classify_shills(self, tweet: str, targets: List[str]) -> Dict[str, Dict[str, str]]:
        """Detect shill signals for multiple targets."""
        return {t: self.classify_shill(tweet, t) for t in targets}


# ============================== Pretty Printers ==============================

def print_general(label: str, d: Dict[str,str]):
    table = Table(title=f"Tweet Sentiment ({label})")
    table.add_column("Decision", justify="center")
    table.add_column("Stance", justify="center")
    table.add_column("Favorable", justify="center")
    table.add_column("Unfavorable", justify="center")
    style = {"POS":"bold green","NEG":"bold red","NEU":"bold yellow"}[d["final"]]
    table.add_row(f"[{style}]{d['final']}[/]", d["stance"], d["favorable"], d["unfavorable"])
    console.print(table); console.print()

def print_targets(label: str, mapping: Dict[str, Dict[str, str]]):
    if not mapping:
        console.print("[yellow]No targets detected.[/]\n")
        return
    table = Table(title=f"Targeted Sentiment ({label})")
    table.add_column("TARGET", style="magenta")
    table.add_column("FINAL", justify="center")
    table.add_column("STANCE", justify="center")
    table.add_column("FAVORABLE", justify="center")
    table.add_column("UNFAVORABLE", justify="center")
    table.add_column("OPTIMISTIC", justify="center")
    for t, d in mapping.items():
        style = {"POS":"bold green","NEG":"bold red","NEU":"bold yellow"}[d["final"]]
        table.add_row(t, f"[{style}]{d['final']}[/]", d["stance"], d["favorable"],
                      d["unfavorable"], d.get("optimistic","-"))
    console.print(table); console.print()

def print_shills(label: str, mapping: Dict[str, Dict[str, str]]):
    if not mapping:
        console.print("[yellow]No targets for shill detection.[/]\n")
        return
    table = Table(title=f"SHILL Detection ({label})")
    table.add_column("TARGET", style="magenta")
    table.add_column("SHILL", justify="center")
    for t, d in mapping.items():
        shill_val = d.get("shill", "NO")
        style = "bold red" if shill_val == "SHILL" else "dim"
        table.add_row(t, f"[{style}]{shill_val}[/]")
    console.print(table); console.print()


# ============================== CLI ==============================

def interactive_main(argv: Optional[List[str]] = None):
    global _running
    ap = argparse.ArgumentParser(description="Bedrock Claude tweet sentiment analyzer")
    ap.add_argument("--model", default=os.getenv("BEDROCK_MODEL","anthropic.claude-3-5-haiku-20241022-v1:0"),
                    help="Bedrock Claude model id")
    ap.add_argument("--region", default=os.getenv("BEDROCK_REGION","ap-northeast-1"),
                    help="Bedrock region (e.g., ap-northeast-1)")
    ap.add_argument("--max-output", type=int, default=int(os.getenv("BEDROCK_MAX_OUTPUT","32")),
                    help="Max tokens for Claude responses (default 32)")
    ap.add_argument("--timeout", type=float, default=float(os.getenv("BEDROCK_TIMEOUT","15")),
                    help="HTTP timeout for Bedrock invoke (seconds)")
    ap.add_argument("--top-p", type=float, default=float(os.getenv("BEDROCK_TOP_P","0.9")),
                    help="Claude top_p value")
    ap.add_argument("--json", action="store_true", help="Print JSON line instead of tables")
    ap.add_argument("--no-targets", action="store_true",
                    help="Disable target-based classification (auto cashtags/parentheses)")
    ap.add_argument("--shill-only", action="store_true",
                    help="Only run shill detection, skip sentiment analysis")
    ap.add_argument("--targets-list", type=str, default="",
                    help="Comma separated manual targets (e.g., BTC,NVDA); merged with auto-detected")
    ap.add_argument("--retries", type=int, default=int(os.getenv("BEDROCK_RETRIES","2")),
                    help="Retry count for backend requests (default 2)")
    ap.add_argument("--temp", type=float, default=float(os.getenv("BEDROCK_TEMP","0.0")),
                    help="Sampling temperature (typically 0.0)")
    ap.add_argument("--rps", type=float, default=float(os.getenv("BEDROCK_RPS","12")),
                    help="Request rate (requests per second), default ~12 rps")
    args = ap.parse_args(argv)

    min_interval = 1.0/args.rps if args.rps > 0 else 0.08

    aws_key = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret = os.getenv("AWS_SECRET_ACCESS_KEY")
    api_key = os.getenv("BEDROCK_API_KEY") or os.getenv("API_KEY")
    bearer_token = os.getenv("BEDROCK_BEARER_TOKEN") or os.getenv("AWS_BEARER_TOKEN_BEDROCK")
    if not ((aws_key and aws_secret) or api_key or bearer_token):
        raise SystemExit(
            "Provide AWS credentials or BEDROCK_API_KEY / BEDROCK_BEARER_TOKEN."
        )

    cfg = BedrockClaudeConfig(
        model_id=args.model,
        region=args.region,
        aws_access_key=aws_key,
        aws_secret_key=aws_secret,
        aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        api_key=api_key,
        bearer_token=bearer_token,
        temp=args.temp,
        top_p=args.top_p,
        max_output_tokens=max(1, args.max_output),
        retries=max(0, args.retries),
        retry_backoff=0.25,
        min_interval=min_interval,
        timeout=max(1.0, args.timeout),
    )
    clf = BedrockClaudeClassifier(cfg)
    backend_label = f"Bedrock ({args.region}): {args.model}"

    console.print(Panel.fit("[bold cyan]Paste a tweet and press Enter. Exit with Ctrl+C[/]"))

    while _running:
        try:
            tweet = read_line_interruptible("Tweet: ")
            if not _running: break
            tweet = (tweet or "").strip()
            if not tweet:
                console.print("(empty line — press Ctrl+C to quit)")
                continue

            # Targets default: enabled
            use_targets = not args.no_targets
            auto = extract_targets_auto(tweet) if use_targets else []
            manual = [t.strip().upper() for t in (args.targets_list.split(",") if args.targets_list else []) if t.strip()]
            targets = sorted({*auto, *manual}) if use_targets else []

            if args.shill_only:
                # Only shill detection
                if targets:
                    shills = clf.classify_shills(tweet, targets)
                    if args.json:
                        print(_json.dumps({
                            "mode": "shill",
                            "model": args.model,
                            "tweet": tweet[:500],
                            "shills": shills,
                            "shilled": [t for t, d in shills.items() if d.get("shill") == "SHILL"]
                        }, ensure_ascii=False))
                    else:
                        print_shills(backend_label, shills)
                else:
                    console.print("[yellow]No targets detected for shill detection.[/]\n")
            elif targets:
                mapping = clf.classify_targets(tweet, targets)
                shills = clf.classify_shills(tweet, targets)
                if args.json:
                    print(_json.dumps({
                        "mode": "targets",
                        "model": args.model,
                        "tweet": tweet[:500],
                        "targets": mapping,
                        "shills": shills,
                        "shilled": [t for t, d in shills.items() if d.get("shill") == "SHILL"]
                    }, ensure_ascii=False))
                else:
                    print_targets(backend_label, mapping)
                    print_shills(backend_label, shills)
            else:
                g = clf.classify_general_multihead(tweet)
                if args.json:
                    print(_json.dumps({
                        "mode": "general",
                        "model": args.model,
                        "tweet": tweet[:500],
                        "general": g
                    }, ensure_ascii=False))
                else:
                    print_general(backend_label, g)

        except KeyboardInterrupt:
            _handle_signal(signal.SIGINT, None); break
        except EOFError:
            break
        except Exception as e:
            console.print(f"[red]Error:[/] {e}")
            continue

    console.print("[bold]Bye![/]")


def main(argv: Optional[List[str]] = None):
    args = list(sys.argv[1:] if argv is None else argv)
    if "--interactive" in args:
        filtered = [arg for arg in args if arg != "--interactive"]
        interactive_main(filtered)
        return

    script_path = Path(__file__).resolve().parent.parent / "sentiment_cli.py"
    if script_path.exists():
        old_argv = sys.argv
        old_path = list(sys.path)
        try:
            sys.path.insert(0, str(script_path.parent))
            sys.argv = [str(script_path), *args]
            runpy.run_path(str(script_path), run_name="__main__")
        finally:
            sys.path[:] = old_path
            sys.argv = old_argv
        return

    console.print("[yellow]External sentiment stream script not found; switching to interactive mode.[/]")
    interactive_main(args)


if __name__ == "__main__":
    main()
