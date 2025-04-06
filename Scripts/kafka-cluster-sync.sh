#!/bin/bash

# Configuration
SOURCE_BROKER="localhost:9092"
TARGET_BROKER="target-kafka:9092"
GROUP_ID="my-group"
TOPIC="my-topic"
PARTITION="0"
KAFKA_BIN="/usr/bin"
TEMP_FILE="/tmp/messages.txt"

# Step 1: Get the current offset and LOG-END-OFFSET from the source cluster
echo "Retrieving offset from source cluster ($SOURCE_BROKER)..."
RAW_OUTPUT=$($KAFKA_BIN/kafka-consumer-groups --bootstrap-server "$SOURCE_BROKER" --group "$GROUP_ID" --describe 2>/dev/null)
echo "Raw output from kafka-consumer-groups (source):"
echo "$RAW_OUTPUT"

OFFSET_INFO=$(echo "$RAW_OUTPUT" | grep "$TOPIC" | grep -w "$PARTITION" | awk '{print $4}')
if [ -z "$OFFSET_INFO" ]; then
  echo "Error: No offset found for group $GROUP_ID, topic $TOPIC, partition $PARTITION."
  exit 1
fi

SOURCE_LOG_END_OFFSET=$(echo "$RAW_OUTPUT" | grep "$TOPIC" | grep -w "$PARTITION" | awk '{print $5}')
if [ -z "$SOURCE_LOG_END_OFFSET" ]; then
  echo "Error: No LOG-END-OFFSET found for group $GROUP_ID, topic $TOPIC, partition $PARTITION."
  exit 1
fi

OFFSET=$OFFSET_INFO
echo "Found offset: $OFFSET and LOG-END-OFFSET: $SOURCE_LOG_END_OFFSET for $TOPIC partition $PARTITION"

# Step 2: Delete and recreate the topic on target to clear messages
echo "Deleting topic $TOPIC on target cluster ($TARGET_BROKER)..."
timeout 10s $KAFKA_BIN/kafka-topics --bootstrap-server "$TARGET_BROKER" --delete --topic "$TOPIC" 2>/dev/null
sleep 2

echo "Recreating topic $TOPIC on target cluster ($TARGET_BROKER)..."
timeout 10s $KAFKA_BIN/kafka-topics --bootstrap-server "$TARGET_BROKER" \
  --create \
  --topic "$TOPIC" \
  --partitions 1 \
  --replication-factor 1

if [ $? -ne 0 ]; then
  echo "Error: Failed to recreate topic $TOPIC on target cluster."
  exit 1
fi
echo "Topic $TOPIC recreated on target cluster."

# Verify topic exists
echo "Verifying topic $TOPIC exists on $TARGET_BROKER..."
timeout 10s $KAFKA_BIN/kafka-topics --bootstrap-server "$TARGET_BROKER" --describe --topic "$TOPIC" > /dev/null
if [ $? -ne 0 ]; then
  echo "Error: Topic $TOPIC not available on $TARGET_BROKER after recreation."
  exit 1
fi

# Step 3: Copy messages from source to target with debug
echo "Copying messages from source ($SOURCE_BROKER) to target ($TARGET_BROKER)..."
$KAFKA_BIN/kafka-console-consumer \
  --bootstrap-server "$SOURCE_BROKER" \
  --topic "$TOPIC" \
  --partition "$PARTITION" \
  --offset earliest \
  --timeout-ms 5000 \
  > "$TEMP_FILE" 2>/dev/null

if [ ! -s "$TEMP_FILE" ]; then
  echo "Error: No messages consumed from source cluster."
  rm -f "$TEMP_FILE"
  exit 1
fi

MESSAGE_COUNT=$(wc -l < "$TEMP_FILE")
echo "Messages consumed to $TEMP_FILE (count: $MESSAGE_COUNT):"
cat "$TEMP_FILE"

echo "Checking connectivity to $TARGET_BROKER..."
nc -zv target-kafka 9092
if [ $? -ne 0 ]; then
  echo "Error: Cannot connect to $TARGET_BROKER."
  rm -f "$TEMP_FILE"
  exit 1
fi

echo "Producing messages to $TARGET_BROKER..."
cat "$TEMP_FILE" | $KAFKA_BIN/kafka-console-producer \
  --bootstrap-server "$TARGET_BROKER" \
  --topic "$TOPIC" \
  --request-required-acks all \
  --sync \
  --property print.value=true \
  --property print.partition=true \
  --property print.timestamp=true \
  --property log.level=DEBUG

PRODUCER_EXIT_CODE=$?
if [ $PRODUCER_EXIT_CODE -ne 0 ]; then
  echo "Error: Failed to produce messages to target cluster. Exit code: $PRODUCER_EXIT_CODE"
  rm -f "$TEMP_FILE"
  exit 1
fi

# Step 4: Sync consumer group (create and set offset)
echo "Syncing consumer group $GROUP_ID on target cluster ($TARGET_BROKER)..."
# Set initial offset to 0 for partition 0 to create the group
timeout 10s $KAFKA_BIN/kafka-consumer-groups --bootstrap-server "$TARGET_BROKER" \
  --group "$GROUP_ID" \
  --topic "$TOPIC:$PARTITION" \
  --reset-offsets \
  --to-offset 0 \
  --execute

if [ $? -ne 0 ]; then
  echo "Error: Failed to initialize $GROUP_ID on target cluster with offset 0 for partition $PARTITION."
  rm -f "$TEMP_FILE"
  exit 1
fi

# Consume one message to ensure group metadata is active
echo -e "\nConsume one message to ensure group metadata is active"
timeout 10s $KAFKA_BIN/kafka-console-consumer \
  --bootstrap-server "$TARGET_BROKER" \
  --group "$GROUP_ID" \
  --topic "$TOPIC" \
  # --partition "$PARTITION" \
  --offset earliest \
  --max-messages 1 \
  --property print.offset=false \
  --property auto.commit.enable=true

if [ $? -ne 0 ]; then
  echo "Warning: Failed to consume a message with $GROUP_ID on target (continuing anyway)."
fi

sleep 2  # Allow offset to commit

# Sync the offset to the source value for partition 0
echo "Setting offset $OFFSET for group $GROUP_ID on target cluster..."
timeout 10s $KAFKA_BIN/kafka-consumer-groups --bootstrap-server "$TARGET_BROKER" \
  --group "$GROUP_ID" \
  --topic "$TOPIC:$PARTITION" \
  --reset-offsets \
  --to-offset "$OFFSET" \
  --execute

if [ $? -ne 0 ]; then
  echo "Error: Failed to set offset $OFFSET on target cluster for partition $PARTITION."
  rm -f "$TEMP_FILE"
  exit 1
fi

# Step 5: Verify message count on target using my-group
echo -e "\nVerifying message count on target cluster ($TARGET_BROKER) with group $GROUP_ID..."
RAW_TARGET_OUTPUT=$(timeout 10s $KAFKA_BIN/kafka-consumer-groups --bootstrap-server "$TARGET_BROKER" --group "$GROUP_ID" --describe 2>/dev/null)
echo "Raw output from kafka-consumer-groups (target):"
echo "$RAW_TARGET_OUTPUT"

TARGET_LOG_END_OFFSET=$(echo "$RAW_TARGET_OUTPUT" | grep "$TOPIC" | grep -w "$PARTITION" | awk '{print $5}')
if [ -z "$TARGET_LOG_END_OFFSET" ]; then
  echo "Error: Failed to get LOG-END-OFFSET from target cluster with $GROUP_ID."
  rm -f "$TEMP_FILE"
  exit 1
fi

if [ "$SOURCE_LOG_END_OFFSET" -ne "$TARGET_LOG_END_OFFSET" ]; then
  echo "Error: Message count mismatch. Source: $SOURCE_LOG_END_OFFSET, Target: $TARGET_LOG_END_OFFSET"
  rm -f "$TEMP_FILE"
  exit 1
fi

echo "Verified: $TARGET_LOG_END_OFFSET messages copied to target"

# Cleanup
rm -f "$TEMP_FILE"
echo "Successfully copied $SOURCE_LOG_END_OFFSET messages and synced offset $OFFSET for group $GROUP_ID to $TARGET_BROKER"