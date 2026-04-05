from .common import *


class NewsAIVerifier:
    """Optional OpenAI-based verifier for investment/partnership news."""

    VALID_POLICIES = {"require", "allow_without_key", "disabled"}

    def __init__(self):
        self.api_key = os.getenv("OPENAI_API_KEY")
        self.base_url = os.getenv("OPENAI_API_BASE", "https://api.openai.com/v1").rstrip("/")
        self.model = os.getenv("TREE_NEWS_AI_MODEL", "gpt-4o-mini")
        self.org_id = os.getenv("OPENAI_ORG_ID")

        try:
            self.timeout = float(os.getenv("TREE_NEWS_AI_TIMEOUT", "4.5"))
        except Exception:
            self.timeout = 4.5

        try:
            self.max_retries = max(1, int(os.getenv("TREE_NEWS_AI_RETRIES", "2")))
        except Exception:
            self.max_retries = 2

        raw_policy = os.getenv("TREE_NEWS_AI_POLICY", "require").strip().lower()
        if raw_policy not in self.VALID_POLICIES:
            logger.warning(
                "Invalid TREE_NEWS_AI_POLICY=%r, falling back to 'require'",
                raw_policy,
            )
            raw_policy = "require"
        self.policy = raw_policy
        self.enabled = bool(self.api_key)
        self._session: Optional[aiohttp.ClientSession] = None

        if self.policy == "disabled":
            logger.info("News AI verifier disabled by TREE_NEWS_AI_POLICY=disabled")
        elif not self.enabled:
            logger.info(
                "News AI verifier missing OPENAI_API_KEY; policy=%s",
                self.policy,
            )

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session and not self._session.closed:
            return self._session

        timeout = ClientTimeout(total=self.timeout + 1.0)
        self._session = aiohttp.ClientSession(timeout=timeout)
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
        self._session = None

    def _disabled_decision(self) -> Tuple[bool, str]:
        if self.policy == "disabled":
            return False, "AI verifier disabled by policy"
        if self.enabled:
            return True, ""
        if self.policy == "allow_without_key":
            return True, "AI verifier bypassed: key missing"
        return False, "AI verifier required but OPENAI_API_KEY is missing"

    async def verify(
        self,
        *,
        data: dict,
        tokens: List[str],
        alert_type: str,
        amount: Optional[float] = None,
        company: Optional[str] = None,
    ) -> Tuple[bool, str]:
        """Return (approved, reason) under an explicit verifier policy."""
        decision, reason = self._disabled_decision()
        if reason:
            return decision, reason

        title = (data.get("title") or "").strip()
        body = (data.get("body") or "").strip()
        url = data.get("url") or ""
        source = data.get("source") or "Unknown"
        created_at = data.get("createdAt") or data.get("publishedAt") or ""

        context_snapshot = {
            "source": source,
            "url": url,
            "alert_type": alert_type,
            "tokens": tokens,
            "amount_millions": amount,
            "company": company,
            "created_at": created_at,
        }

        system_prompt = (
            "You are an analyst that validates crypto investment and strategic partnership news for an automated trading bot. "
            "Approve when the article confirms a NEW, official investment, capital raise (including PIPE/follow-on rounds, commitments, or convertible financing), treasury purchase, or strategic partnership tied to the provided tokens or their issuing project. "
            "Token buybacks, treasury allocations, or foundation-led market purchases count as valid investment events. "
            "Reject anything speculative, recycled, opinion-based, indirect, or unrelated to the project backing the tokens. "
            "If the source looks unverified or lacks a concrete announcement, reject. "
            "Respond with 'APPROVE: <reason>' or 'REJECT: <reason>'. Keep the reason under 120 characters."
        )

        user_prompt = (
            f"Context: {json.dumps(context_snapshot, ensure_ascii=False)}\n"
            f"Tokens Provided: {', '.join(tokens)}\n"
            f"Title: {title}\n"
            f"Body: {body[:4000]}\n"
            "Question: Is this article announcing a NEW confirmed deal that matches the alert type for these tokens or their project?"
        )

        payload = {
            "model": self.model,
            "temperature": 0,
            "max_tokens": 120,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        if self.org_id:
            headers["OpenAI-Organization"] = self.org_id

        endpoint = f"{self.base_url}/chat/completions"

        for attempt in range(1, self.max_retries + 1):
            try:
                session = await self._get_session()
                timeout = ClientTimeout(total=self.timeout)
                async with session.post(
                    endpoint,
                    json=payload,
                    headers=headers,
                    timeout=timeout,
                ) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        logger.warning("AI verifier HTTP %s: %s", resp.status, text[:200])
                        continue

                    result = await resp.json()
                    content = (
                        (result.get("choices") or [{}])[0]
                        .get("message", {})
                        .get("content", "")
                    )
                    parsed_decision, parsed_reason = self._parse_decision(content)
                    if parsed_decision is None:
                        logger.warning(
                            "AI verifier could not parse response: %s",
                            content,
                        )
                        continue

                    approved = parsed_decision == "APPROVE"
                    return approved, parsed_reason

            except asyncio.TimeoutError:
                logger.warning("AI verifier timeout on attempt %s", attempt)
            except aiohttp.ClientError as exc:
                logger.warning("AI verifier network error on attempt %s: %s", attempt, exc)
            except Exception as exc:
                logger.exception("AI verifier unexpected error: %s", exc)

        return False, "AI verification failed"

    @staticmethod
    def _parse_decision(content: str) -> Tuple[Optional[str], str]:
        if not content:
            return None, ""

        text = content.strip()
        upper = text.upper()

        if upper.startswith("APPROVE"):
            reason = text.split(":", 1)[1].strip() if ":" in text else ""
            return "APPROVE", reason
        if upper.startswith("REJECT"):
            reason = text.split(":", 1)[1].strip() if ":" in text else ""
            return "REJECT", reason

        return None, ""
