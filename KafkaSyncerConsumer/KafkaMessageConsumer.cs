using Confluent.Kafka;

namespace KafkaSyncerConsumer;

public class KafkaMessageConsumer
{
    private readonly string _bootstrapServers;
    private readonly string _topic;
    private readonly string _groupId;

    public KafkaMessageConsumer(string bootstrapServers, string topic, string groupId)
    {
        _bootstrapServers = bootstrapServers;
        _topic = topic;
        _groupId = groupId;
    }

    public async Task ConsumeMessagesAsync(CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _groupId,
            //Debug = "all",
            AutoOffsetReset = AutoOffsetReset.Earliest, // Start consuming from the beginning if no offset is stored
            EnableAutoCommit = true, // Enable autocommit
            AutoCommitIntervalMs = 5000 //Set autocommit interval to 5 seconds
        };

        try
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
            consumer.Subscribe(_topic);

            Console.WriteLine($"Consuming messages from topic {_topic}...");

            try
            {
                var topicPartition = new TopicPartition(_topic, 0);
                //WatermarkOffsets watermarkOffsets = consumer.GetWatermarkOffsets(topicPartition);
                //Console.WriteLine($"{watermarkOffsets.High.Value} - {watermarkOffsets.Low.Value}");
                
                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(cancellationToken);

                    if (consumeResult != null && consumeResult.Message != null)
                    {
                        Console.WriteLine($"Received message: {consumeResult.Message.Value}");
                    }
                    await Task.Delay(1000, cancellationToken);

                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Consumer cancelled.");
            }
            finally
            {
                consumer.Close();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Consumer error: {ex.Message}");
        }

    }

    public async Task ReReadAllMessagesSameGroupAsync(CancellationToken cancellationToken)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = _groupId, // Use the SAME group id
            EnableAutoOffsetStore = false, // Disable auto offset store
            EnableAutoCommit = false, // Disable autocommit for manual control
            AutoOffsetReset = AutoOffsetReset.Earliest // Start from beginning if no offset is found
        };

        var assignmentEvent = new TaskCompletionSource<List<TopicPartition>>();

        using (var consumer = new ConsumerBuilder<Ignore, string>(config)
                   .SetPartitionsAssignedHandler((c, partitions) =>
                   {
                       Console.WriteLine($"Assigned partitions: [{string.Join(", ", partitions)}]");
                       assignmentEvent.TrySetResult(partitions.ToList());
                   })
                   .Build())
        {
            consumer.Subscribe(_topic);

            CancellationTokenRegistration registration = cancellationToken.Register(() =>
            {
                assignmentEvent.TrySetCanceled();
            });

            try
            {
                // Add a timeout to the assignment task
                Task<List<TopicPartition>> assignmentTask = assignmentEvent.Task;
                Task timeoutTask = Task.Delay(TimeSpan.FromSeconds(2), cancellationToken); // Timeout after 10 seconds

                Task completedTask = await Task.WhenAny(assignmentTask, timeoutTask);

                // if (completedTask == timeoutTask)
                // {
                //     Console.WriteLine("Timeout waiting for partition assignment.");
                //     return; // Or throw an exception if appropriate
                // }
                //
                // List<TopicPartition> assignedPartitions = await assignmentTask;

                Console.WriteLine($"Re-reading all messages from topic {_topic} with the same group id {_groupId}...");

                // Seek to the beginning of each partition
                foreach (var topicPartition in assignmentTask.Result)
                {
                    var lowWatermark = consumer.GetWatermarkOffsets(topicPartition).Low;
                    consumer.Seek(new TopicPartitionOffset(topicPartition, lowWatermark));
                }

                while (!cancellationToken.IsCancellationRequested)
                {
                    var consumeResult = consumer.Consume(cancellationToken);

                    if (consumeResult != null && consumeResult.Message != null)
                    {
                        Console.WriteLine(
                            $"Re-read message: {consumeResult.Message.Value}, Offset: {consumeResult.Offset}");
                        // Process the message as needed.
                    }
                    else if (consumeResult.IsPartitionEOF)
                    {
                        Console.WriteLine($"Reached end of partition: {consumeResult.TopicPartition}");
                    }
                    else if (consumeResult == null)
                    {
                        // Timeout occurred, but no error.  Just continue the loop.
                        continue;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Re-read cancelled.");
            }
            catch (AggregateException ex) when (ex.InnerException is TaskCanceledException)
            {
                Console.WriteLine("Re-read cancelled due to timeout or cancellation.");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Re-read error: {ex.Message}");
            }
            finally
            {
                registration.Dispose();
                consumer.Close();
            }
        }
    }

    public static Task<DateTime?> GetEarliestMessageTimestamp(string bootstrapServers, string topic, int partition)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = Guid.NewGuid().ToString(), // Unique group
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
        var topicPartition = new TopicPartition(topic, partition);
        var beginningOffsets = consumer.GetWatermarkOffsets(topicPartition);
        var beginningOffset = beginningOffsets.Low;
        if (beginningOffset == Offset.End) return null; // Partition is empty

        consumer.Assign(new List<TopicPartitionOffset>
            { new TopicPartitionOffset(topicPartition, beginningOffset.Value) });

        try
        {
            var consumeResult = consumer.Consume(TimeSpan.FromSeconds(10));

            if (consumeResult?.Message != null)
            {
                return Task.FromResult<DateTime?>(consumeResult.Message.Timestamp.UtcDateTime);
            }
            else
            {
                Console.WriteLine($"Could not consume first message from {topic}-{partition}");
                return Task.FromResult<DateTime?>(null);
            }
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Consume error: {e.Error.Reason}");
            return null;
        }
        finally
        {
            consumer.Close();
        }
    }

    public static Task<DateTime?> GetLatestMessageTimestamp(string bootstrapServers, string topic, int partition)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = Guid.NewGuid().ToString(), // Unique group
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Latest
        };

        using (var consumer = new ConsumerBuilder<Ignore, string>(config).Build())
        {
            var topicPartition = new TopicPartition(topic, partition);
            var endOffsets = consumer.GetWatermarkOffsets(topicPartition);
            var endOffset = endOffsets.High;
            if (endOffset == Offset.Beginning) return null!; // Partition is empty

            consumer.Assign(new List<TopicPartitionOffset>
                { new TopicPartitionOffset(topicPartition, endOffset - 1) }); // Subtract 1 to get to latest message

            try
            {
                var consumeResult = consumer.Consume(TimeSpan.FromSeconds(10));

                if (consumeResult?.Message != null)
                {
                    return Task.FromResult<DateTime?>(consumeResult.Message.Timestamp.UtcDateTime);
                }
                else
                {
                    Console.WriteLine($"Could not consume last message from {topic}-{partition}");
                    return null;
                }
            }
            catch (ConsumeException e)
            {
                Console.WriteLine($"Consume error: {e.Error.Reason}");
                return null;
            }
            finally
            {
                consumer.Close();
            }
        }
    }

    public async Task<List<ConsumeResult<TKey, TValue>>> ReadAllMessagesFromBeginningAsync<TKey, TValue>(
        string topicName,
        string consumerGroupId,
        CancellationToken cancellationToken = default)
    {
        var config = new ConsumerConfig
        {
            BootstrapServers = _bootstrapServers,
            GroupId = consumerGroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest // Still useful as a fallback
        };

        var adminConfig = new AdminClientConfig { BootstrapServers = _bootstrapServers };

        var allMessages = new List<ConsumeResult<TKey, TValue>>();

        using (var adminClient = new AdminClientBuilder(adminConfig).Build())
        using (var consumer = new ConsumerBuilder<TKey, TValue>(config).Build())
        {
            consumer.Subscribe(topicName);

            try
            {
                // Wait for the consumer to be assigned partitions
                while (consumer.Assignment.Count == 0 && !cancellationToken.IsCancellationRequested)
                {
                    consumer.Consume(TimeSpan.FromMilliseconds(100)); // Poll to trigger assignment
                    await Task.Delay(100); // Small delay to avoid busy-waiting
                }

                if (cancellationToken.IsCancellationRequested)
                {
                    Console.WriteLine("Operation cancelled before partition assignment.");
                    return allMessages;
                }

                var assignedPartitions = consumer.Assignment.Where(tp => tp.Topic == topicName).ToList();

                if (!assignedPartitions.Any())
                {
                    Console.WriteLine($"No partitions assigned for topic '{topicName}'.");
                    return allMessages;
                }

                // Seek to the beginning of each assigned partition
                foreach (var tp in assignedPartitions)
                {
                    consumer.Seek(new TopicPartitionOffset(tp, Offset.Beginning));
                }

                Console.WriteLine(
                    $"Consumer for group '{consumerGroupId}' rewound to the beginning of topic '{topicName}'. Assigned partitions: {string.Join(", ", assignedPartitions)}");

                while (!cancellationToken.IsCancellationRequested)
                {
                    try
                    {
                        var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100)); // Short timeout
                        if (consumeResult?.Message != null)
                        {
                            allMessages.Add(consumeResult);
                            Console.WriteLine(
                                $"Read message at {consumeResult.TopicPartitionOffset}: Key = {consumeResult.Message.Key}, Value = {consumeResult.Message.Value}");
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error during consumption: {e.Error.Reason}");
                        break;
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Consumption cancelled.");
            }
            catch (Exception e)
            {
                Console.WriteLine($"An error occurred: {e}");
            }
            finally
            {
                consumer.Close();
                adminClient.Dispose();
            }
        }

        return allMessages;
    }
}