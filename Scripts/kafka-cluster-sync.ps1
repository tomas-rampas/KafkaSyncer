# Configuration
$SOURCE_BROKER = "localhost:9092"
$TARGET_BROKER = "localhost:9094"
$GROUP_ID = "my-group"
$TOPIC = "my-topic"
$PARTITION = "0"
$KAFKA_BASE = "D:\apps\kafka_2.13-4.0.0"
$KAFKA_BIN = "$KAFKA_BASE\bin\windows\"
$TEMP_FILE = "D:\tmp\messages.txt"

# Ensure the temp directory exists
$tempDir = Split-Path $TEMP_FILE -Parent
if (-not (Test-Path $tempDir))
{
    New-Item -Path $tempDir -ItemType Directory -Force
}

# Step 1: Get the current offset and LOG-END-OFFSET from the source cluster
Write-Host "Retrieving offset from source cluster ($SOURCE_BROKER)..."
$RAW_OUTPUT = & "${KAFKA_BIN}kafka-consumer-groups.bat" --bootstrap-server $SOURCE_BROKER --group $GROUP_ID --describe # 2>$null
Write-Host "Raw output from kafka-consumer-groups (source):"
Write-Host $RAW_OUTPUT

$OFFSET_INFO = ($RAW_OUTPUT -split "`n" | Where-Object { $_ -match $TOPIC -and $_ -match "\s+$PARTITION\s+" } | ForEach-Object { ($_ -split "\s+")[3] })
if (-not $OFFSET_INFO)
{
    Write-Host "Error: No offset found for group $GROUP_ID, topic $TOPIC, partition $PARTITION."
    exit 1
}

$SOURCE_LOG_END_OFFSET = ($RAW_OUTPUT -split "`n" | Where-Object { $_ -match $TOPIC -and $_ -match "\s+$PARTITION\s+" } | ForEach-Object { ($_ -split "\s+")[4] })
if (-not $SOURCE_LOG_END_OFFSET)
{
    Write-Host "Error: No LOG-END-OFFSET found for group $GROUP_ID, topic $TOPIC, partition $PARTITION."
    exit 1
}

$OFFSET = $OFFSET_INFO
Write-Host "Found offset: $OFFSET and LOG-END-OFFSET: $SOURCE_LOG_END_OFFSET for $TOPIC partition $PARTITION"

# Step 2: Delete and recreate the topic on target to clear messages
Write-Host "Deleting topic $TOPIC on target cluster ($TARGET_BROKER)..."
Start-Process -FilePath "${KAFKA_BIN}kafka-topics.bat" -ArgumentList "--bootstrap-server $TARGET_BROKER --delete --topic $TOPIC" -NoNewWindow -Wait -RedirectStandardError "NUL"
Start-Sleep -Seconds 2

Write-Host "Recreating topic $TOPIC on target cluster ($TARGET_BROKER)..."
$createProcess = Start-Process -FilePath "${KAFKA_BIN}kafka-topics.bat" -ArgumentList "--bootstrap-server $TARGET_BROKER --create --topic $TOPIC --partitions 1 --replication-factor 1" -NoNewWindow -Wait -PassThru
if ($createProcess.ExitCode -ne 0)
{
    Write-Host "Error: Failed to recreate topic $TOPIC on target cluster."
    exit 1
}
Write-Host "Topic $TOPIC recreated on target cluster."

# Verify topic exists
Write-Host "Verifying topic $TOPIC exists on $TARGET_BROKER..."
$verifyProcess = Start-Process -FilePath "${KAFKA_BIN}kafka-topics.bat" -ArgumentList "--bootstrap-server $TARGET_BROKER --describe --topic $TOPIC" -NoNewWindow -Wait -RedirectStandardOutput "NUL" -PassThru
if ($verifyProcess.ExitCode -ne 0)
{
    Write-Host "Error: Topic $TOPIC not available on $TARGET_BROKER after recreation."
    exit 1
}

# Step 3: Copy messages from source to target
Write-Host "Copying messages from source ($SOURCE_BROKER) to target ($TARGET_BROKER)..."
& "${KAFKA_BIN}kafka-console-consumer.bat" --bootstrap-server $SOURCE_BROKER --topic $TOPIC --partition $PARTITION --offset earliest --timeout-ms 5000 > $TEMP_FILE 2> $null

if (-not (Test-Path $TEMP_FILE) -or (Get-Item $TEMP_FILE).Length -eq 0)
{
    Write-Host "Error: No messages consumed from source cluster."
    Remove-Item -Path $TEMP_FILE -Force -ErrorAction SilentlyContinue
    exit 1
}

$MESSAGE_COUNT = (Get-Content $TEMP_FILE | Measure-Object -Line).Lines
Write-Host "Messages consumed to $TEMP_FILE (count: $MESSAGE_COUNT):"
Get-Content $TEMP_FILE

Write-Host "Checking connectivity to $TARGET_BROKER..."
try
{
    $tcp = New-Object System.Net.Sockets.TcpClient
    $tcp.Connect("localhost", 9094)
    $tcp.Close()
}
catch
{
    Write-Host "Error: Cannot connect to $TARGET_BROKER."
    Remove-Item -Path $TEMP_FILE -Force -ErrorAction SilentlyContinue
    exit 1
}

Write-Host "Producing messages to $TARGET_BROKER..."
Get-Content $TEMP_FILE | & "${KAFKA_BIN}kafka-console-producer.bat" --bootstrap-server $TARGET_BROKER `
    --topic $TOPIC --request-required-acks all --sync --property print.value=true --property print.partition=true `
    --property print.timestamp=true --property log.level=DEBUG

$PRODUCER_EXIT_CODE = $LASTEXITCODE
if ($PRODUCER_EXIT_CODE -ne 0)
{
    Write-Host "Error: Failed to produce messages to target cluster. Exit code: $PRODUCER_EXIT_CODE"
    Remove-Item -Path $TEMP_FILE -Force -ErrorAction SilentlyContinue
    exit 1
}

# Step 4: Sync consumer group (create and set offset)
Write-Host "Syncing consumer group $GROUP_ID on target cluster ($TARGET_BROKER)..."
# Set initial offset to 0 for partition 0 to create the group
$resetProcess = Start-Process -FilePath "${KAFKA_BIN}kafka-consumer-groups.bat" -ArgumentList "--bootstrap-server $TARGET_BROKER --group $GROUP_ID --topic ${TOPIC}:${PARTITION} --reset-offsets --to-offset 0 --execute" -NoNewWindow -Wait -PassThru
if ($resetProcess.ExitCode -ne 0)
{
    Write-Host "Error: Failed to initialize $GROUP_ID on target cluster with offset 0 for partition $PARTITION."
    Remove-Item -Path $TEMP_FILE -Force -ErrorAction SilentlyContinue
    exit 1
}

Start-Sleep -Seconds 2  # Allow offset to commit

# Sync the offset to the source value for partition 0
Write-Host "Setting offset $OFFSET for group $GROUP_ID on target cluster..."
$syncProcess = Start-Process -FilePath "${KAFKA_BIN}kafka-consumer-groups.bat" -ArgumentList "--bootstrap-server $TARGET_BROKER --group $GROUP_ID --topic ${TOPIC}:${PARTITION} --reset-offsets --to-offset $OFFSET --execute" -NoNewWindow -Wait -PassThru
if ($syncProcess.ExitCode -ne 0)
{
    Write-Host "Error: Failed to set offset $OFFSET on target cluster for partition $PARTITION."
    Remove-Item -Path $TEMP_FILE -Force -ErrorAction SilentlyContinue
    exit 1
}

# Step 5: Verify message count on target using my-group
Write-Host "`nVerifying message count on target cluster ($TARGET_BROKER) with group $GROUP_ID..."
$RAW_TARGET_OUTPUT = & "${KAFKA_BIN}kafka-consumer-groups.bat" --bootstrap-server $TARGET_BROKER --group $GROUP_ID --describe 2> $null
Write-Host "Raw output from kafka-consumer-groups (target):"
Write-Host $RAW_TARGET_OUTPUT

$TARGET_LOG_END_OFFSET = ($RAW_TARGET_OUTPUT -split "`n" | Where-Object { $_ -match $TOPIC -and $_ -match "\s+$PARTITION\s+" } | ForEach-Object { ($_ -split "\s+")[4] })
if (-not $TARGET_LOG_END_OFFSET)
{
    Write-Host "Error: Failed to get LOG-END-OFFSET from target cluster with $GROUP_ID."
    Remove-Item -Path $TEMP_FILE -Force -ErrorAction SilentlyContinue
    exit 1
}

if ([int]$SOURCE_LOG_END_OFFSET -ne [int]$TARGET_LOG_END_OFFSET)
{
    Write-Host "Error: Message count mismatch. Source: $SOURCE_LOG_END_OFFSET, Target: $TARGET_LOG_END_OFFSET"
    Remove-Item -Path $TEMP_FILE -Force -ErrorAction SilentlyContinue
    exit 1
}

Write-Host "Verified: $TARGET_LOG_END_OFFSET messages copied to target"

# Cleanup
Remove-Item -Path $TEMP_FILE -Force -ErrorAction SilentlyContinue
Write-Host "Successfully copied $SOURCE_LOG_END_OFFSET messages and synced offset $OFFSET for group $GROUP_ID to $TARGET_BROKER"