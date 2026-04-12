"""Base Kafka Consumer with common utilities."""
import json
import os
import signal
import threading
import time
from abc import ABC, abstractmethod
from typing import List
from unittest.mock import MagicMock

from kafka import KafkaConsumer
from prometheus_client import Counter, Histogram, generate_latest, CollectorRegistry

from shared.db_utils import get_db, get_kafka_producer, redis_client

# ── Config ──
KAFKA_BROKER_URL = os.environ.get("KAFKA_BROKER_URL", "kafka:29092")
METRICS_PORT = int(os.environ.get("METRICS_PORT", "8003"))
POLL_TIMEOUT_MS = int(os.environ.get("POLL_TIMEOUT_MS", "1000"))


class BaseConsumer(ABC):
    """Base class for all Kafka consumers with metrics and lifecycle management."""

    def __init__(self, topics: List[str], group_id: str, metrics_port: int = None, mock_consumer: bool = False):
        self.topics = topics
        self.group_id = group_id
        self.metrics_port = metrics_port or METRICS_PORT
        self.running = True

        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

        # Initialize Kafka consumer (skip if mocking for tests)
        if mock_consumer:
            self.consumer = MagicMock()
            print(f"🎧 {self.__class__.__name__} using MOCK consumer (test mode)")
        else:
            self.consumer = KafkaConsumer(
                *topics,
                bootstrap_servers=KAFKA_BROKER_URL,
                group_id=group_id,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                consumer_timeout_ms=POLL_TIMEOUT_MS,
            )
            print(f"🎧 {self.__class__.__name__} listening to: {topics} (group={group_id})")

        # Metrics - use separate registry for test isolation
        registry = CollectorRegistry() if mock_consumer else None
        
        # Metrics
        self.events_processed = Counter(
            f"{self._metric_prefix}_processed_total",
            "Total events processed",
            ["event_type"],
            registry=registry,
        )
        self.events_failed = Counter(
            f"{self._metric_prefix}_failed_total",
            "Total events failed",
            ["event_type"],
            registry=registry,
        )
        self.processing_time = Histogram(
            f"{self._metric_prefix}_processing_seconds",
            "Event processing time",
            registry=registry,
        )

    @property
    @abstractmethod
    def _metric_prefix(self) -> str:
        """Metric name prefix for this consumer."""
        pass

    @abstractmethod
    def handle_event(self, message: dict):
        """Handle a single Kafka message. Must be implemented by subclasses."""
        pass

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        print(f"\n🛑 {self.__class__.__name__} received signal {signum}, shutting down...")
        self.running = False
        self.consumer.close()

    def start(self):
        """Start consuming messages."""
        print(f"✅ {self.__class__.__name__} started")

        # Start metrics server in background thread
        self._start_metrics_server()

        # Main consumption loop
        while self.running:
            try:
                messages = self.consumer.poll(timeout_ms=POLL_TIMEOUT_MS, max_records=10)
                for topic_partition, records in messages.items():
                    for record in records:
                        message = record.value
                        self._process_message(message)
            except Exception as e:
                print(f"❌ Error in consumption loop: {e}")
                import traceback
                traceback.print_exc()
                time.sleep(1)

        print(f"🛑 {self.__class__.__name__} stopped")

    def _process_message(self, message: dict):
        """Process a single message with metrics tracking."""
        event_type = message.get("event_type", "unknown")
        order_id = message.get("order_id", "unknown")

        print(f"\n📨 [{self.__class__.__name__}] Received {event_type} for {order_id}")

        start_time = time.time()
        try:
            self.handle_event(message)
            elapsed = time.time() - start_time
            self.processing_time.observe(elapsed)
            self.events_processed.labels(event_type=event_type).inc()
            print(f"  ✅ [{self.__class__.__name__}] Processed {event_type} for {order_id} ({elapsed:.2f}s)")
        except Exception as e:
            elapsed = time.time() - start_time
            self.processing_time.observe(elapsed)
            self.events_failed.labels(event_type=event_type).inc()
            print(f"  ❌ [{self.__class__.__name__}] Failed to process {event_type} for {order_id}: {e}")
            import traceback
            traceback.print_exc()
            # Don't re-raise — let consumer continue with next message
            # Errors should be handled within handle_event implementation

    def _start_metrics_server(self):
        """Start Prometheus metrics server in background thread."""
        from http.server import HTTPServer, BaseHTTPRequestHandler

        class MetricsHandler(BaseHTTPRequestHandler):
            def do_GET(self):
                if self.path == "/metrics":
                    self.send_response(200)
                    self.send_header("Content-Type", "text/plain; charset=utf-8")
                    self.end_headers()
                    self.wfile.write(generate_latest())
                else:
                    self.send_response(404)
                    self.end_headers()

            def log_message(self, fmt, *args):
                pass  # Suppress logs

        def run_server():
            server = HTTPServer(("0.0.0.0", self.metrics_port), MetricsHandler)
            print(f"📊 Metrics server started on port {self.metrics_port}")
            server.serve_forever()

        thread = threading.Thread(target=run_server, daemon=True)
        thread.start()
