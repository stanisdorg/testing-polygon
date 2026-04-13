"""Shared gRPC server for all consumers.

Provides Health and ConsumerStatus gRPC services.
Runs in a background thread alongside the main consumer loop.
"""
import os
import threading
from concurrent import futures
from datetime import datetime, timezone

import grpc

# Generated gRPC code
import health_pb2
import health_pb2_grpc
import consumer_status_pb2
import consumer_status_pb2_grpc

GRPC_PORT = int(os.environ.get("GRPC_PORT", 50051))
CONSUMER_NAME = os.environ.get("CONSUMER_NAME", "unknown")


class HealthServicer(health_pb2_grpc.HealthServicer):
    """Standard health check for gRPC."""

    def Check(self, request, context):
        return health_pb2.HealthCheckResponse(
            status=health_pb2.HealthCheckResponse.SERVING
        )


class ConsumerStatusServicer(consumer_status_pb2_grpc.ConsumerStatusServicer):
    """Returns consumer processing status."""

    # Class-level shared state (updated by consumer main loop)
    _processed_count = 0
    _last_event_type = ""
    _lock = threading.Lock()

    @classmethod
    def update_stats(cls, event_type: str):
        """Call this from consumer main loop after processing an event."""
        with cls._lock:
            cls._processed_count += 1
            cls._last_event_type = event_type

    @classmethod
    def reset_stats(cls):
        with cls._lock:
            cls._processed_count = 0
            cls._last_event_type = ""

    def GetStatus(self, request, context):
        with self._lock:
            return consumer_status_pb2.StatusResponse(
                consumer_name=CONSUMER_NAME,
                processed_count=self._processed_count,
                last_event_type=self._last_event_type,
            )


def start_grpc_server(port: int = GRPC_PORT) -> grpc.Server:
    """Запускает gRPC сервер в отдельном потоке. Возвращает объект сервера."""
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    health_pb2_grpc.add_HealthServicer_to_server(HealthServicer(), server)
    consumer_status_pb2_grpc.add_ConsumerStatusServicer_to_server(
        ConsumerStatusServicer(), server
    )
    server.add_insecure_port(f"[::]:{port}")
    server.start()
    print(f"  🟢 gRPC server started on port {port}")
    return server


def get_grpc_status_update_fn():
    """Возвращает функцию для обновления статистики из консьюмера."""
    return ConsumerStatusServicer.update_stats
