"""Queue service for managing episode processing.

Episodes are spooled to disk before processing and only removed on success,
so an LLM-backend outage can no longer drop memories (325 episodes were
silently lost this way over five months when the proxy at :11437 was down).

Spool layout:
    ~/.graphiti/queue/pending/  - awaiting processing or mid-retry
    ~/.graphiti/queue/dead/     - exhausted MAX_ATTEMPTS; requeued with a
                                  fresh attempt budget on next server restart
                                  (a restart implies someone fixed the backend)
"""

import asyncio
import json
import logging
import os
import uuid as uuid_module
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from graphiti_core.nodes import EpisodeType

logger = logging.getLogger(__name__)

# 10 attempts with capped exponential backoff spans ~2.5h of outage — long
# enough to ride out a proxy restart, bounded so a poison episode can't spin
# forever (it lands in dead/ and only re-cycles on server restart).
MAX_ATTEMPTS = 10
BACKOFF_BASE_SECONDS = 30.0
BACKOFF_CAP_SECONDS = 1800.0

SPOOL_ROOT = Path(os.environ.get('GRAPHITI_QUEUE_DIR', str(Path.home() / '.graphiti' / 'queue')))


@dataclass
class EpisodeRecord:
    """Serializable unit of work — everything needed to re-run add_episode.

    entity_types is deliberately NOT part of the record: it is global config
    (same for every episode) and not JSON-serializable, so it lives on the
    service and is injected at process time.
    """

    record_id: str
    group_id: str
    name: str
    content: str
    source_description: str
    episode_type: str  # EpisodeType enum name
    uuid: str | None
    enqueued_at: str  # ISO-8601 UTC; reused as reference_time so retries and replays keep the original timeline
    attempts: int = 0


class QueueService:
    """Sequential per-group episode processing with a durable disk spool."""

    def __init__(self):
        self._episode_queues: dict[str, asyncio.Queue] = {}
        self._queue_workers: dict[str, bool] = {}
        self._graphiti_client: Any = None
        self._entity_types: Any = None
        self._pending_dir = SPOOL_ROOT / 'pending'
        self._dead_dir = SPOOL_ROOT / 'dead'

    # -- spool primitives ---------------------------------------------------

    def _spool_path(self, record: EpisodeRecord) -> Path:
        return self._pending_dir / f'{record.record_id}.json'

    def _spool(self, record: EpisodeRecord) -> None:
        """Write (or rewrite, after a failed attempt) the record's spool file atomically."""
        path = self._spool_path(record)
        tmp = path.with_suffix('.tmp')
        tmp.write_text(json.dumps(asdict(record), ensure_ascii=False, indent=2))
        os.replace(tmp, path)

    def _unspool(self, record: EpisodeRecord) -> None:
        self._spool_path(record).unlink(missing_ok=True)

    def _bury(self, record: EpisodeRecord) -> None:
        """Move an exhausted record to dead/. Never deletes — dead/ is the audit trail."""
        self._spool(record)  # persist final attempt count
        os.replace(self._spool_path(record), self._dead_dir / f'{record.record_id}.json')
        logger.error(
            f'Episode "{record.name}" (group {record.group_id}) moved to dead-letter '
            f'after {record.attempts} attempts: {self._dead_dir / record.record_id}.json'
        )

    def _recover_spooled(self) -> list[EpisodeRecord]:
        """Load all spooled records at startup: pending as-is, dead with a fresh attempt budget."""
        records: list[EpisodeRecord] = []
        for directory, reset_attempts in ((self._dead_dir, True), (self._pending_dir, False)):
            for path in directory.glob('*.json'):
                try:
                    record = EpisodeRecord(**json.loads(path.read_text()))
                except (json.JSONDecodeError, TypeError) as e:
                    corrupt = self._dead_dir / f'{path.stem}.corrupt'
                    logger.error(f'Unreadable spool file {path} ({e}) — parked at {corrupt}')
                    os.replace(path, corrupt)
                    continue
                if reset_attempts:
                    record.attempts = 0
                    os.replace(path, self._pending_dir / path.name)
                records.append(record)
        records.sort(key=lambda r: r.enqueued_at)
        return records

    # -- queue mechanics ----------------------------------------------------

    async def _enqueue(self, record: EpisodeRecord) -> int:
        if record.group_id not in self._episode_queues:
            self._episode_queues[record.group_id] = asyncio.Queue()
        await self._episode_queues[record.group_id].put(record)
        if not self._queue_workers.get(record.group_id, False):
            asyncio.create_task(self._process_episode_queue(record.group_id))
        return self._episode_queues[record.group_id].qsize()

    async def _requeue_after(self, record: EpisodeRecord, delay: float) -> None:
        await asyncio.sleep(delay)
        logger.info(
            f'Retrying episode "{record.name}" (group {record.group_id}, '
            f'attempt {record.attempts + 1}/{MAX_ATTEMPTS})'
        )
        await self._enqueue(record)

    async def _process_episode_queue(self, group_id: str) -> None:
        """Long-lived worker: processes one group's episodes sequentially."""
        logger.info(f'Starting episode queue worker for group_id: {group_id}')
        self._queue_workers[group_id] = True
        try:
            while True:
                record = await self._episode_queues[group_id].get()
                try:
                    await self._process(record)
                    self._unspool(record)
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    record.attempts += 1
                    logger.error(
                        f'Failed to process episode "{record.name}" for group {group_id} '
                        f'(attempt {record.attempts}/{MAX_ATTEMPTS}): {e}'
                    )
                    if record.attempts >= MAX_ATTEMPTS:
                        self._bury(record)
                    else:
                        self._spool(record)  # persist the attempt count
                        delay = min(
                            BACKOFF_BASE_SECONDS * 2 ** (record.attempts - 1),
                            BACKOFF_CAP_SECONDS,
                        )
                        asyncio.create_task(self._requeue_after(record, delay))
                finally:
                    self._episode_queues[group_id].task_done()
        except asyncio.CancelledError:
            logger.info(f'Episode queue worker for group_id {group_id} was cancelled')
        except Exception as e:
            logger.error(f'Unexpected error in queue worker for group_id {group_id}: {e}')
        finally:
            self._queue_workers[group_id] = False
            logger.info(f'Stopped episode queue worker for group_id: {group_id}')

    async def _process(self, record: EpisodeRecord) -> None:
        logger.info(f'Processing episode "{record.name}" ({record.record_id}) for group {record.group_id}')
        try:
            episode_type = EpisodeType[record.episode_type]
        except KeyError:
            episode_type = EpisodeType.text
        await self._graphiti_client.add_episode(
            name=record.name,
            episode_body=record.content,
            source_description=record.source_description,
            source=episode_type,
            group_id=record.group_id,
            reference_time=datetime.fromisoformat(record.enqueued_at),
            entity_types=self._entity_types,
            uuid=record.uuid,
        )
        logger.info(f'Successfully processed episode "{record.name}" for group {record.group_id}')

    # -- public API ----------------------------------------------------------

    async def initialize(self, graphiti_client: Any, entity_types: Any = None) -> None:
        """Wire up the graphiti client and requeue anything spooled from prior runs."""
        self._graphiti_client = graphiti_client
        self._entity_types = entity_types
        self._pending_dir.mkdir(parents=True, exist_ok=True)
        self._dead_dir.mkdir(parents=True, exist_ok=True)
        recovered = self._recover_spooled()
        for record in recovered:
            await self._enqueue(record)
        if recovered:
            logger.info(f'Recovered {len(recovered)} spooled episode(s) from previous runs')
        logger.info('Queue service initialized with graphiti client')

    async def add_episode(
        self,
        group_id: str,
        name: str,
        content: str,
        source_description: str,
        episode_type: Any,
        uuid: str | None,
    ) -> int:
        """Spool an episode to disk and queue it for processing.

        Returns the position in the group's queue. The spool file is the
        durability guarantee: it exists before this method returns and is
        only removed after graphiti persists the episode.
        """
        if self._graphiti_client is None:
            raise RuntimeError('Queue service not initialized. Call initialize() first.')

        record = EpisodeRecord(
            record_id=uuid_module.uuid4().hex,
            group_id=group_id,
            name=name,
            content=content,
            source_description=source_description,
            episode_type=episode_type.name if isinstance(episode_type, EpisodeType) else str(episode_type),
            uuid=uuid,
            enqueued_at=datetime.now(timezone.utc).isoformat(),
        )
        self._spool(record)
        return await self._enqueue(record)

    def get_queue_size(self, group_id: str) -> int:
        if group_id not in self._episode_queues:
            return 0
        return self._episode_queues[group_id].qsize()

    def is_worker_running(self, group_id: str) -> bool:
        return self._queue_workers.get(group_id, False)
