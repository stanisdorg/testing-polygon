#!/bin/bash
# Initialize Kafka topics for FulfilBox event-driven architecture

KAFKA_BROKER="kafka:29092"

echo "🔄 Creating Kafka topics..."

# Wait for Kafka to be ready
echo "⏳ Waiting for Kafka to be ready..."
until kafka-topics --bootstrap-server $KAFKA_BROKER --list > /dev/null 2>&1; do
    echo "   Waiting for Kafka..."
    sleep 2
done

echo "✅ Kafka is ready!"

# Create topics if they don't exist
topics=(
    "order_events:1:1"
    "inventory_events:1:1"
    "payment_events:1:1"
    "warehouse_events:1:1"
    "delivery_events:1:1"
    "fulfilment.audit:3:1"
)

for topic_config in "${topics[@]}"; do
    IFS=':' read -r topic partitions replicas <<< "$topic_config"
    
    # Check if topic exists
    if kafka-topics --bootstrap-server $KAFKA_BROKER --describe --topic $topic > /dev/null 2>&1; then
        echo "  ✓ Topic '$topic' already exists"
    else
        kafka-topics --bootstrap-server $KAFKA_BROKER \
            --create \
            --topic $topic \
            --partitions $partitions \
            --replication-factor $replicas
        echo "  ✅ Created topic '$topic' (partitions=$partitions, replicas=$replicas)"
    fi
done

echo ""
echo "📋 Kafka Topics Summary:"
kafka-topics --bootstrap-server $KAFKA_BROKER --list
echo ""
echo "🎉 Kafka topics initialization complete!"
