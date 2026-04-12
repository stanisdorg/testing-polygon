"""Test Kafka topic viewer in Student Portal."""
import json
from fastapi.testclient import TestClient

from portal import app, config

client = TestClient(app)


class TestKafkaTopicsAPI:
    """Tests for Kafka topics API endpoints."""

    def test_kafka_topics_list(self):
        """GET /api/kafka/topics returns configured topics."""
        response = client.get("/api/kafka/topics")
        assert response.status_code == 200
        data = response.json()
        assert "topics" in data
        assert "fulfilment.events" in data["topics"]

    def test_kafka_config_has_fulfilment_events(self):
        """Config includes fulfilment.events topic."""
        assert "fulfilment.events" in config["kafka"]["topics"]

    def test_kafka_config_has_order_events(self):
        """Config includes order_events topic."""
        assert "order_events" in config["kafka"]["topics"]

    def test_kafka_config_has_order_events_dlq(self):
        """Config includes order_events_dlq topic."""
        assert "order_events_dlq" in config["kafka"]["topics"]

    def test_kafka_topics_count(self):
        """Config includes exactly 3 topics."""
        assert len(config["kafka"]["topics"]) == 3

    def test_kafka_topics_order(self):
        """Topics are in expected order."""
        expected = ["order_events", "fulfilment.events", "order_events_dlq"]
        assert config["kafka"]["topics"] == expected


class TestKafkaMessagesAPI:
    """Tests for Kafka messages API endpoints."""

    def test_kafka_latest_endpoint_exists(self):
        """GET /api/kafka/{topic}/latest returns valid response structure."""
        response = client.get("/api/kafka/fulfilment.events/latest")
        assert response.status_code == 200
        data = response.json()
        assert "topic" in data
        assert data["topic"] == "fulfilment.events"
        assert "messages" in data
        assert isinstance(data["messages"], list)

    def test_kafka_latest_with_count_param(self):
        """GET /api/kafka/{topic}/latest accepts count parameter."""
        response = client.get("/api/kafka/order_events/latest?count=5")
        assert response.status_code == 200
        data = response.json()
        assert data["topic"] == "order_events"
        assert isinstance(data["messages"], list)

    def test_kafka_topics_endpoint_returns_all_configured_topics(self):
        """GET /api/kafka/topics returns all topics from config."""
        response = client.get("/api/kafka/topics")
        assert response.status_code == 200
        data = response.json()
        for topic in config["kafka"]["topics"]:
            assert topic in data["topics"]


class TestKafkaConfig:
    """Tests for Kafka configuration."""

    def test_kafka_enabled_in_config(self):
        """Kafka is enabled in config."""
        assert config["kafka"]["enabled"] is True

    def test_kafka_bootstrap_servers(self):
        """Kafka bootstrap servers is configured."""
        assert "bootstrap_servers" in config["kafka"]
        assert config["kafka"]["bootstrap_servers"] == "kafka:29092"

    def test_kafka_readonly_mode(self):
        """Kafka is in readonly mode."""
        assert config["kafka"]["readonly"] is True


class TestKafkaTopicDetailsAPI:
    """Tests for Kafka topic details API endpoints."""

    def test_topic_details_endpoint_exists(self):
        """GET /api/kafka/topic/{topic}/details returns valid response structure."""
        response = client.get("/api/kafka/topic/fulfilment.events/details")
        assert response.status_code == 200
        data = response.json()
        assert "topic" in data
        assert data["topic"] == "fulfilment.events"

    def test_topic_details_has_partition_info_or_error(self):
        """Topic details returns either partition info or error (if Kafka not reachable)."""
        response = client.get("/api/kafka/topic/order_events/details")
        assert response.status_code == 200
        data = response.json()
        # Either has partition data or error
        assert "error" in data or "partitions" in data

    def test_topic_details_nonexistent_topic(self):
        """Topic details for nonexistent topic returns error."""
        response = client.get("/api/kafka/topic/nonexistent_topic_xyz/details")
        assert response.status_code == 200
        data = response.json()
        # Should return error or empty partitions
        assert "error" in data or "partition_count" in data

    def test_topic_details_has_topic_name(self):
        """Topic details returns the requested topic name."""
        response = client.get("/api/kafka/topic/fulfilment.events/details")
        data = response.json()
        assert data["topic"] == "fulfilment.events"


class TestKafkaConsumerGroupsAPI:
    """Tests for Kafka consumer groups API endpoints."""

    def test_consumer_groups_endpoint_exists(self):
        """GET /api/kafka/consumer-groups returns valid response structure."""
        response = client.get("/api/kafka/consumer-groups")
        assert response.status_code == 200
        data = response.json()
        assert "groups" in data
        assert isinstance(data["groups"], list)

    def test_consumer_groups_returns_list(self):
        """Consumer groups endpoint always returns a list."""
        response = client.get("/api/kafka/consumer-groups")
        assert response.status_code == 200
        data = response.json()
        # May be empty list or list with error object
        assert isinstance(data["groups"], list)
