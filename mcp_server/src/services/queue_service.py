"""Queue service for managing episode processing.

Episodes are spooled to disk before processing and only removed on success,
so an LLM-backend outage can no longer drop memories (325 episodes were
silently lost this way over five months when the proxy at :11437 was down).

Spool layout (root overridable via GRAPHITI_QUEUE_DIR):
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


@dataclass
class EpisodeRecord:
    """Serializable unit of work — everything needed to re-run add_episode.

    entity_types / edge_types / edge_type_map are deliberately NOT part of the
    record: they are Pydantic model registries (not JSON-serializable) and in
    practice global config. They are held in memory per record while the
    process lives, and replays after a restart fall back to the service-level
    registries injected via initialize().

    All fields added after the initial deployment default to None/False so
    spool files written by the old format still deserialize.
    """

    record_id: str
    group_id: str
    name: str
    content: str
    source_description: str
    episode_type: str  # EpisodeType enum name
    uuid: str | None
    enqueued_at: str  # ISO-8601 UTC; fallback reference_time so retries and replays keep the original timeline
    attempts: int = 0
    reference_time: str | None = None  # ISO-8601; caller-supplied event time (bi-temporal model)
    excluded_entity_types: list[str] | None = None
    previous_episode_uuids: list[str] | None = None
    custom_extraction_instructions: str | None = None
    update_communities: bool = False
    saga: str | None = None
    saga_previous_episode_uuid: str | None = None


class QueueService:
    """Sequential per-group episode processing with a durable disk spool."""

    def __init__(self):
        self._episode_queues: dict[str, asyncio.Queue] = {}
        self._queue_workers: dict[str, bool] = {}
        self._graphiti_client: Any = None
        # Service-level type registries, used when replaying spooled records
        # whose in-memory registries were lost to a restart.
        self._entity_types: Any = None
        self._edge_types: Any = None
        self._edge_type_map: Any = None
        # record_id -> (entity_types, edge_types, edge_type_map) for records
        # enqueued by this process; cleared on success or burial.
        self._live_types: dict[str, tuple[Any, Any, Any]] = {}
        spool_root = Path(
            os.environ.get('GRAPHITI_QUEUE_DIR', str(Path.home() / '.graphiti' / 'queue'))
        )
        self._pending_dir = spool_root / 'pending'
        self._dead_dir = spool_root / 'dead'

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
        self._live_types.pop(record.record_id, None)

    def _bury(self, record: EpisodeRecord) -> None:
        """Move an exhausted record to dead/. Never deletes — dead/ is the audit trail."""
        self._spool(record)  # persist final attempt count
        os.replace(self._spool_path(record), self._dead_dir / f'{record.record_id}.json')
        self._live_types.pop(record.record_id, None)
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
            # Claim the flag before create_task runs: rapid enqueues (startup
            # recovery) would otherwise spawn duplicate workers per group and
            # break sequential-per-group processing.
            self._queue_workers[record.group_id] = True
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
        logger.info(
            f'Processing episode "{record.name}" ({record.record_id}) for group {record.group_id}'
        )
        try:
            episode_type = EpisodeType[record.episode_type]
        except KeyError:
            episode_type = EpisodeType.text
        entity_types, edge_types, edge_type_map = self._live_types.get(
            record.record_id, (self._entity_types, self._edge_types, self._edge_type_map)
        )
        await self._graphiti_client.add_episode(
            name=record.name,
            episode_body=record.content,
            source_description=record.source_description,
            source=episode_type,
            group_id=record.group_id,
            reference_time=datetime.fromisoformat(record.reference_time or record.enqueued_at),
            entity_types=entity_types,
            edge_types=edge_types,
            edge_type_map=edge_type_map,
            excluded_entity_types=record.excluded_entity_types,
            previous_episode_uuids=record.previous_episode_uuids,
            custom_extraction_instructions=record.custom_extraction_instructions,
            update_communities=record.update_communities,
            saga=record.saga,
            saga_previous_episode_uuid=record.saga_previous_episode_uuid,
            uuid=record.uuid,
        )
        logger.info(f'Successfully processed episode "{record.name}" for group {record.group_id}')

    # -- public API ----------------------------------------------------------

    async def initialize(
        self,
        graphiti_client: Any,
        entity_types: Any = None,
        edge_types: Any = None,
        edge_type_map: Any = None,
    ) -> None:
        """Wire up the graphiti client and requeue anything spooled from prior runs.

        The type registries are the replay defaults for spooled records that
        outlived the process which enqueued them.
        """
        self._graphiti_client = graphiti_client
        self._entity_types = entity_types
        self._edge_types = edge_types
        self._edge_type_map = edge_type_map
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
        entity_types: Any,
        uuid: str | None,
        reference_time: datetime | None = None,
        edge_types: Any = None,
        edge_type_map: Any = None,
        excluded_entity_types: list[str] | None = None,
        previous_episode_uuids: list[str] | None = None,
        custom_extraction_instructions: str | None = None,
        update_communities: bool = False,
        saga: str | None = None,
        saga_previous_episode_uuid: str | None = None,
    ) -> int:
        """Spool an episode to disk and queue it for processing.

        Args:
            group_id: The group ID for the episode
            name: Name of the episode
            content: Episode content
            source_description: Description of the episode source
            episode_type: Type of the episode
            entity_types: Entity types for extraction
            uuid: Episode UUID
            reference_time: Event occurrence time for the episode. Defaults to
                the enqueue time when not provided, so retries and restart
                replays keep the original timeline (bi-temporal model).
            edge_types: Optional mapping of edge (fact) type name to Pydantic model
            edge_type_map: Optional mapping of (source, target) entity type pairs to
                allowed edge type names
            excluded_entity_types: Optional list of entity type names to exclude
                from extraction
            previous_episode_uuids: Optional explicit list of prior episode UUIDs to
                use as context (overrides automatic retrieval)
            custom_extraction_instructions: Optional extra natural-language
                instructions for the extraction LLM
            update_communities: Whether to incrementally update communities after
                ingestion
            saga: Optional saga name/id to attach this episode to
            saga_previous_episode_uuid: Optional UUID of the prior episode in the saga

        Returns:
            The position in the group's queue. The spool file is the
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
            episode_type=episode_type.name
            if isinstance(episode_type, EpisodeType)
            else str(episode_type),
            uuid=uuid,
            enqueued_at=datetime.now(timezone.utc).isoformat(),
            reference_time=reference_time.isoformat() if reference_time else None,
            excluded_entity_types=excluded_entity_types,
            previous_episode_uuids=previous_episode_uuids,
            custom_extraction_instructions=custom_extraction_instructions,
            update_communities=update_communities,
            saga=saga,
            saga_previous_episode_uuid=saga_previous_episode_uuid,
        )
        self._live_types[record.record_id] = (entity_types, edge_types, edge_type_map)
        self._spool(record)
        return await self._enqueue(record)

    def get_queue_size(self, group_id: str) -> int:
        if group_id not in self._episode_queues:
            return 0
        return self._episode_queues[group_id].qsize()

    def is_worker_running(self, group_id: str) -> bool:
        return self._queue_workers.get(group_id, False)
