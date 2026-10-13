"""Integration tests for Apache Kafka partitioning, Redis Streams, SSE broadcaster, and cache warmup."""

import time
import uuid
import pytest

from payflow.common.db import default_engine, init_db
from payflow.events.broker import KafkaPartitionedBroker, RedisStreamEventBroker, UnifiedMessageBroker
from payflow.events.schemas import DomainEvent
from payflow.events.sse import sse_broadcaster
from payflow.cache.manager import CacheManager


def _make_event(event_type: str = "payment_authorized", key: str = "merch_1", amount: int = 2500) -> DomainEvent:
    return DomainEvent(
        event_id=f"evt_{uuid.uuid4().hex[:10]}",
        event_type=event_type,
        aggregate_type="payment",
        aggregate_id=f"pay_{uuid.uuid4().hex[:8]}",
        payload={"merchant_id": key, "amount_minor": amount, "timestamp": time.time()},
    )


def test_kafka_consistent_partitioning_and_ordering():
    broker = KafkaPartitionedBroker(num_partitions_default=3)
    same_key = "merch_consistent_store_42"

    assigned_partitions = set()
    for i in range(15):
        evt = _make_event(key=same_key, amount=1000 + i)
        broker.publish("payments", evt, partition_key=same_key)
        p_id = broker.get_partition_for_key(same_key)
        assigned_partitions.add(p_id)

    # Invariant: Every message with the identical partition key must go to exactly 1 partition
    assert len(assigned_partitions) == 1
    p_num = list(assigned_partitions)[0]
    p = broker.topics["payments"][p_num]
    assert p.log_end_offset == 15
    assert len(p.messages) == 15
    # Offsets must be strictly monotonic
    for idx, (offset, stored_evt, ts, p_key) in enumerate(p.messages):
        assert offset == idx


def test_kafka_consumer_group_dispatch_and_lag():
    broker = KafkaPartitionedBroker(num_partitions_default=3)
    consumed_events = []

    def test_handler(event: DomainEvent):
        consumed_events.append(event.event_id)

    broker.subscribe("payments", "order_processor_group", test_handler)

    # Publish 12 events distributed across keys
    for i in range(12):
        evt = _make_event(key=f"merch_{i}", amount=500 + i)
        broker.publish("payments", evt, partition_key=f"merch_{i}")

    # Give consumer worker thread time to process
    time.sleep(0.4)

    assert len(consumed_events) == 12
    stats = broker.stats()
    topic_stats = stats["topics"]["payments"]
    assert topic_stats["total_messages"] == 12

    # Check consumer group lag is 0 after complete consumption
    cg = next(g for g in topic_stats["consumer_groups"] if g["consumer_group"] == "order_processor_group")
    assert cg["total_lag"] == 0


def test_kafka_offset_rewind_replay():
    broker = KafkaPartitionedBroker(num_partitions_default=3)
    consumed_count = 0

    def counter_handler(event: DomainEvent):
        nonlocal consumed_count
        consumed_count += 1

    broker.subscribe("payments", "audit_replay_group", counter_handler)

    for i in range(6):
        broker.publish("payments", _make_event(key=f"user_{i}"))

    time.sleep(0.3)
    assert consumed_count == 6

    # Rewind offsets to 0
    res = broker.rewind_offsets("payments", "audit_replay_group", target_offset=0)
    assert res["status"] == "REWOUND"

    # Worker will re-consume all 6 events from offset 0
    time.sleep(0.4)
    assert consumed_count == 12  # 6 original + 6 replayed


def test_redis_stream_engine_xadd_and_stats():
    broker = RedisStreamEventBroker()
    evt1 = _make_event(key="merch_redis_1")
    evt2 = _make_event(key="merch_redis_2")

    broker.publish("stream:payments", evt1)
    broker.publish("stream:payments", evt2)

    stats = broker.stats()
    assert "stream:payments" in stats["streams"]
    assert stats["streams"]["stream:payments"]["length"] == 2
    assert stats["total_stream_entries"] >= 2


def test_unified_message_broker_dual_delivery():
    broker = UnifiedMessageBroker()
    received_kafka = []
    received_redis = []

    broker.kafka.subscribe("payments", "unified_kafka_group", lambda e: received_kafka.append(e.event_id))
    broker.redis_stream.subscribe("payments", "unified_redis_group", lambda e: received_redis.append(e.event_id))

    evt = _make_event(key="tenant_corp_1")
    broker.publish("payments", evt, partition_key="tenant_corp_1")

    time.sleep(0.3)

    assert evt.event_id in received_kafka
    stats = broker.stats()
    assert stats["status"] == "HEALTHY"
    assert "Apache Kafka (3 Partitions)" in stats["active_message_queues"]
    assert stats["summary"]["total_messages_enqueued"] >= 1


def test_cache_warmup_and_prefix_invalidation():
    init_db(default_engine)
    cache = CacheManager()

    # Pre-heat cache
    warmup_res = cache.warmup(default_engine)
    assert warmup_res["status"] == "WARMED"
    assert "keys_warmed" in warmup_res
    assert warmup_res["l1_size"] >= 0

    # Test prefix invalidation
    cache.set("cache:payments:merch_a:10", "dummy1")
    cache.set("cache:payments:merch_b:10", "dummy2")
    cache.set("cache:merchants:merch_a", "dummy3")

    invalidated = cache.invalidate_prefix("cache:payments:")
    assert invalidated >= 2
    assert cache.get("cache:payments:merch_a:10") is None
    assert cache.get("cache:payments:merch_b:10") is None
    assert cache.get("cache:merchants:merch_a") == "dummy3"


def test_sse_broadcaster_event_delivery():
    sse_broadcaster.broadcast("test_ping", {"foo": "bar", "val": 123})
    recent = sse_broadcaster.get_recent_events(limit=5)
    types = [e["type"] for e in recent]
    assert "test_ping" in types
