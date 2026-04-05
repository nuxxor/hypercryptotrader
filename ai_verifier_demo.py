#!/usr/bin/env python3
"""Feed sample news payloads to NewsAIVerifier on a 5 second cadence."""

import asyncio
import os
import time

from tree_news import NewsAIVerifier


SAMPLE_CASES = [
    {
        "name": "PayPal integrates Flow",
        "alert_type": "partnership",
        "tokens": ["FLOW"],
        "company": "PAYPAL",
        "data": {
            "title": "PayPal rolls out Flow blockchain support for P2P crypto transfers",
            "body": (
                "PayPal announced a strategic integration with Flow, allowing US users to send"
                " and receive FLOW tokens through the PayPal wallet. The rollout references official"
                " documentation and a co-authored statement from PayPal and Flow developer Dapper Labs."
            ),
            "source": "Reuters",
            "url": "https://www.reuters.com/technology/paypal-flow-crypto-integration-2025-09-18/",
            "createdAt": "2025-09-18T19:20:00Z",
        },
    },
    {
        "name": "Speculative Google rumor",
        "alert_type": "partnership",
        "tokens": ["LINK"],
        "company": "GOOGLE",
        "data": {
            "title": "Community chatter hints at new Google partnership with Chainlink",
            "body": (
                "Several influencers on social media claim Google Cloud is preparing an integration"
                " with Chainlink oracles, but the posts cite unnamed sources and no official blog or"
                " filing. Chainlink Labs declined to comment."
            ),
            "source": "Blogs",
            "url": "https://cryptobuzz.blog/google-chainlink-rumor",
            "createdAt": "2025-09-18T18:00:00Z",
        },
    },
    {
        "name": "Polygon Series D raise",
        "alert_type": "investment",
        "tokens": ["MATIC"],
        "amount": 250.0,
        "data": {
            "title": "Polygon secures $250M strategic round led by BlackRock",
            "body": (
                "Layer-2 network Polygon has closed a $250 million Series D round led by BlackRock"
                " with participation from Sequoia and Temasek. Management stated the funds will be"
                " used to expand Polygon CDK adoption."
            ),
            "source": "Bloomberg",
            "url": "https://www.bloomberg.com/news/articles/2025-09-18/polygon-raises-250-million-series-d",
            "createdAt": "2025-09-18T17:45:00Z",
        },
    },
    {
        "name": "Security incident misclassified",
        "alert_type": "investment",
        "tokens": ["SOL"],
        "amount": 80.0,
        "data": {
            "title": "Solana Foundation commits $80M to remediation after validator exploit",
            "body": (
                "Following a validator exploit, the Solana Foundation allocated $80 million from its"
                " treasury to reimburse affected users and fund a security review."
            ),
            "source": "Cointelegraph",
            "url": "https://cointelegraph.com/news/solana-foundation-responds-to-validator-exploit",
            "createdAt": "2025-09-18T16:30:00Z",
        },
    },
    {
        "name": "Lagrange Foundation repost",
        "alert_type": "partnership",
        "tokens": ["FLOW"],
        "company": "PAYPAL",
        "data": {
            "title": "Two major tech companies announced this week new crypto integrations",
            "body": (
                "Google released its Agent Payments Protocol. PayPal announced crypto integration"
                " into its P2P payment flow. Source unclear."
            ),
            "source": "Unknown",
            "url": "https://lagrangenews.world/post/agent-payments-protocol",
            "createdAt": "2025-09-18T19:23:45Z",
        },
    },
    {
        "name": "Proper treasury purchase",
        "alert_type": "investment",
        "tokens": ["BTC"],
        "amount": 150.0,
        "data": {
            "title": "NVIDIA allocates $150M to Bitcoin for long-term treasury diversification",
            "body": (
                "In an 8-K filing, NVIDIA disclosed a $150 million Bitcoin purchase as part of a new"
                " treasury diversification strategy effective immediately."
            ),
            "source": "SEC",
            "url": "https://www.sec.gov/ixviewer/doc/NVIDIA-2025-bitcoin-allocation",
            "createdAt": "2025-09-18T15:12:00Z",
        },
    },
    {
        "name": "Marketing fluff",
        "alert_type": "partnership",
        "tokens": ["ARB"],
        "company": "MICROSOFT",
        "data": {
            "title": "Microsoft mentions Arbitrum ecosystem in conference panel",
            "body": (
                "During a gaming conference Q&A, a Microsoft developer advocate referenced Arbitrum"
                " as an example of scaling tech but clarified there is no official partnership."
            ),
            "source": "Blogs",
            "url": "https://gamingdaily.blog/microsoft-arbitrum-conference-notes",
            "createdAt": "2025-09-18T14:00:00Z",
        },
    },
    {
        "name": "Ethena follow-on PIPE and buyback",
        "alert_type": "investment",
        "tokens": ["ENA"],
        "amount": 530.0,
        "data": {
            "title": "Ethena announces $530M StablecoinX PIPE deal, foundation adds $310M buyback",
            "body": (
                "Ethena Labs disclosed it has priced a $530 million follow-on PIPE for its StablecoinX"
                " initiative with participation from existing backers, while the Ethena Foundation"
                " separately authorized a $310 million open market token buyback program."
            ),
            "source": "Press",
            "url": "https://ethena.com/news/stablecoinx-pipe-530m",
            "createdAt": "2025-09-18T19:55:00Z",
        },
    },
    {
        "name": "Numerai JPMorgan commitment",
        "alert_type": "investment",
        "tokens": ["NMR"],
        "amount": 500.0,
        "data": {
            "title": "Numerai secures up to $500M commitment from JPMorgan",
            "body": (
                "San Francisco-based Numerai LLC said it has secured a commitment of as much as"
                " $500 million from JPMorgan Chase, according to Bloomberg, to scale its tokenized"
                " hedge fund infrastructure."
            ),
            "source": "Bloomberg",
            "url": "https://www.bloomberg.com/news/articles/2025-09-18/numerai-secures-500m-commitment-from-jpmorgan",
            "createdAt": "2025-09-18T20:05:00Z",
        },
    },
]


async def evaluate_case(verifier: NewsAIVerifier, case: dict, index: int, total: int) -> None:
    start = time.perf_counter()
    approved, reason = await verifier.verify(
        data=case["data"],
        tokens=case["tokens"],
        alert_type=case["alert_type"],
        amount=case.get("amount"),
        company=case.get("company"),
    )
    elapsed = time.perf_counter() - start
    status = "APPROVED" if approved else "REJECTED"
    name = case["name"]
    print(
        f"[{index}/{total}] {name}: {status} in {elapsed:.2f}s | reason: {reason or 'n/a'}"
    )


async def run_demo(interval: float = 5.0) -> None:
    verifier = NewsAIVerifier()
    total = len(SAMPLE_CASES)
    try:
        for idx, case in enumerate(SAMPLE_CASES, 1):
            await evaluate_case(verifier, case, idx, total)
            if idx < total:
                await asyncio.sleep(interval)
    finally:
        await verifier.close()


def main() -> None:
    try:
        interval = float(os.getenv("AI_VERIFIER_INTERVAL", "5"))
    except Exception:
        interval = 5.0
    asyncio.run(run_demo(interval))


if __name__ == "__main__":
    main()
