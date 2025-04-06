using Confluent.Kafka;

namespace KafkaSyncer;

public static partial class KafkaSync
{
    public static async Task VerifySync(string sourceBootstrapServers, string destinationBootstrapServers,
        string topicName, string groupId = "my-group")
    {
        Console.WriteLine($"Starting verification for topic {topicName} and group {groupId}...");

        try
        {
            // 1. Get the number of messages in the source topic.
            long sourceMessageCount =
                await GetMessageCountAsync(sourceBootstrapServers, topicName, groupId);
            Console.WriteLine($"Source topic {topicName} has {sourceMessageCount} messages.");

            // 2. Consume all messages from the destination topic using the specified consumer group.
            List<string> destinationMessages =
                await ConsumeAllMessagesAsync(destinationBootstrapServers, topicName, groupId);
            long destinationMessageCount = destinationMessages.Count;
            Console.WriteLine($"Destination topic {topicName} has {destinationMessageCount} messages.");

            // 3. Compare the number of messages.
            if (sourceMessageCount != destinationMessageCount)
            {
                Console.WriteLine(
                    $"ERROR: Message count mismatch. Source: {sourceMessageCount}, Destination: {destinationMessageCount}");
                return;
            }

            Console.WriteLine("Message count matches. Now validating message content...");

            // 4. Consume messages from the source topic (again, unfortunately, to compare content).
            List<string> sourceMessages =
                await ConsumeAllMessagesAsync(sourceBootstrapServers, topicName, groupId + "-verification-temp");

            // 5. Validate the content of the messages.
            for (int i = 0; i < sourceMessageCount; i++)
            {
                if (sourceMessages[i] != destinationMessages[i])
                {
                    Console.WriteLine($"ERROR: Message content mismatch at index {i}.");
                    Console.WriteLine($"  Source: {sourceMessages[i]}");
                    Console.WriteLine($"  Destination: {destinationMessages[i]}");
                    return;
                }
            }

            Console.WriteLine($"Topic {topicName} for group {groupId} successfully verified!");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred during verification: {ex.Message}");
        }
    }

    private static async Task<long> GetMessageCountAsync(string bootstrapServers, string topicName, string groupId)
    {
        long messageCount = 0;

        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var adminClient = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        var topicMetadata = adminClient.GetMetadata(topicName, TimeSpan.FromSeconds(10)).Topics[0];

        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        foreach (var partitionMetadata in topicMetadata.Partitions)
        {
            var topicPartition = new TopicPartition(topicName, partitionMetadata.PartitionId);
            WatermarkOffsets watermarkOffsets = consumer.GetWatermarkOffsets(topicPartition);
            messageCount += watermarkOffsets.High.Value - watermarkOffsets.Low.Value;
        }
        consumer.Close();

        return messageCount;
    }
    private static async Task<List<string>> ConsumeAllMessagesAsync(string bootstrapServers, string topicName,
        string groupId)
    {
        var messages = new List<string>();
        var config = new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = false,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoOffsetStore = false
        };

        using (var consumer = new ConsumerBuilder<string, string>(config).Build())
        {
            consumer.Subscribe(topicName);

            try
            {
                while (true)
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100));

                    if (consumeResult == null || consumeResult.IsPartitionEOF)
                    {
                        break;
                    }

                    if (consumeResult.Message != null)
                    {
                        messages.Add(consumeResult.Message.Value);
                    }

                    consumer.Commit(consumeResult);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error during consumption: {ex.Message}");
            }
            finally
            {
                consumer.Close();
            }
        }

        return messages;
    }
}