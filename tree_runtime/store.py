from .common import *


class PersistentSignalStore:
    """Persistent signal deduplication store with batched atomic flushes."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.signals: Dict[str, Dict[str, Any]] = {}
        self._dirty = False
        self._flush_task: Optional[asyncio.Task] = None
        self._flush_interval = TREE_NEWS_STORE_FLUSH_INTERVAL
        self._max_entries = max(
            1, int(os.getenv("TREE_NEWS_SIGNAL_STORE_MAX_ENTRIES", "10000"))
        )
        self.load()

    def load(self):
        """Load signals from disk (backward compatible)."""
        try:
            with open(self.filepath, "r", encoding="utf-8") as f:
                raw = json.load(f)
            normalized: Dict[str, Dict[str, Any]] = {}
            for key, value in raw.items():
                if isinstance(value, dict):
                    normalized[key] = value
                else:
                    normalized[key] = {
                        "received_utc_iso": value,
                        "legacy": True,
                    }
            self.signals = normalized
            logger.info(
                "Loaded %d processed signals (normalized)",
                len(self.signals),
            )
        except FileNotFoundError:
            logger.info("Starting with clean signal store")
        except Exception as exc:
            logger.error("Error loading signals: %s", exc)

    def contains(self, signal_hash: str) -> bool:
        return signal_hash in self.signals

    def _prune(self) -> None:
        if len(self.signals) <= self._max_entries:
            return

        def _sort_key(item: Tuple[str, Dict[str, Any]]) -> str:
            _, data = item
            return data.get("received_utc_iso") or data.get("signal_timestamp_iso") or ""

        sorted_items = sorted(self.signals.items(), key=_sort_key)
        self.signals = dict(sorted_items[-self._max_entries :])

    def _mark_dirty(self) -> None:
        self._dirty = True
        self._schedule_flush()

    def _schedule_flush(self) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return

        if self._flush_task is None or self._flush_task.done():
            self._flush_task = loop.create_task(self._delayed_flush())

    async def _delayed_flush(self) -> None:
        try:
            await asyncio.sleep(self._flush_interval)
            await self.flush()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # pragma: no cover - defensive logging
            logger.error("Signal store flush task failed: %s", exc)

    def add(self, signal_hash: str, meta: Dict[str, Any]):
        self.signals[signal_hash] = meta
        self._prune()
        self._mark_dirty()

    def update_many(self, entries: Dict[str, Dict[str, Any]]) -> None:
        if not entries:
            return
        self.signals.update(entries)
        self._prune()
        self._mark_dirty()

    def remove(self, signal_hash: str) -> None:
        if signal_hash in self.signals:
            self.signals.pop(signal_hash, None)
            self._mark_dirty()

    def save(self) -> None:
        snapshot = dict(self.signals)
        self._write_snapshot(snapshot)
        self._dirty = False

    def _write_snapshot(self, snapshot: Dict[str, Dict[str, Any]]) -> None:
        try:
            path = Path(self.filepath)
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp = path.with_suffix(path.suffix + ".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(snapshot, f, indent=2)
            os.replace(tmp, path)
        except Exception as exc:
            logger.error("Error saving signals: %s", exc)
            raise

    async def flush(self, *, force: bool = False) -> None:
        if not self._dirty and not force:
            return

        snapshot = dict(self.signals)
        self._dirty = False
        try:
            await asyncio.to_thread(self._write_snapshot, snapshot)
        except Exception:
            self._dirty = True
            raise

    async def close(self) -> None:
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            await asyncio.gather(self._flush_task, return_exceptions=True)
        self._flush_task = None
        await self.flush(force=True)

    def get_stats(self) -> Dict[str, int]:
        stats: Dict[str, int] = {}
        for hash_key in self.signals.keys():
            source = hash_key.split(":", 1)[0]
            stats[source] = stats.get(source, 0) + 1
        return stats
