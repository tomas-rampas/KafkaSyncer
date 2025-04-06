using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaSyncer;

public static partial class KafkaSync
{
    public static async Task SyncKafkaAsync(string sourceBootstrapServers, string destinationBootstrapServers,
        string groupId = "my-group", bool deleteExisting = true)
    {
        Console.WriteLine("Starting Kafka synchronization...");

        try
        {
            // 1. Get topic configurations from the source Kafka cluster.
            var topicConfigurations = await GetTopicConfigurationsAsync(sourceBootstrapServers);

            // 2. Handle topics in the destination Kafka cluster, creating or deleting as needed.
            await HandleTopicsAsync(destinationBootstrapServers, topicConfigurations, deleteExisting);

            // 3. **Set consumer group offsets to the beginning in the *destination* cluster BEFORE replication.**
            await ResetConsumerGroupOffsetsToBeginningAsync(destinationBootstrapServers, groupId, topicConfigurations);

            // 4. Replicate data from source to destination.
            await ReplicateDataAsync(sourceBootstrapServers, destinationBootstrapServers, topicConfigurations);

            // 5. Sync consumer group offsets from the source to the destination Kafka cluster.
            //Remove or use to keep progress
            //await SyncConsumerGroupOffsetsAsync(sourceBootstrapServers, destinationBootstrapServers, groupId);

            Console.WriteLine("Kafka synchronization completed.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred: {ex.Message}");
        }
    }

//Extract method that set all consumer group offsets to the beginning
    public static async Task ResetConsumerGroupOffsetsToBeginningAsync(string destinationBootstrapServers,
        string groupId, List<TopicSpecification> topicConfigurations)
    {
        Console.WriteLine($"Starting consumer group offset reset for group: {groupId}...");

        using (var adminClient =
               new AdminClientBuilder(new AdminClientConfig { BootstrapServers = destinationBootstrapServers }).Build())
        {
            foreach (var topicConfiguration in topicConfigurations)
            {
                try
                {
                    var topicPartitionOffsetList = new List<TopicPartitionOffset>();
                    for (int partition = 0; partition < topicConfiguration.NumPartitions; partition++)
                    {
                        topicPartitionOffsetList.Add(
                            new TopicPartitionOffset(new TopicPartition(topicConfiguration.Name, partition),
                                Offset.Beginning));
                    }

                    var consumerGroupTopicPartitionOffsets =
                        new ConsumerGroupTopicPartitionOffsets(groupId, topicPartitionOffsetList);
                    await adminClient.AlterConsumerGroupOffsetsAsync(
                        new List<ConsumerGroupTopicPartitionOffsets>() { consumerGroupTopicPartitionOffsets },
                        new AlterConsumerGroupOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(30) });
                    Console.WriteLine(
                        $"Successfully set consumer group {groupId} offsets to beginning for topic {topicConfiguration.Name}.");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(
                        $"Error setting consumer group {groupId} offsets to beginning for topic {topicConfiguration.Name}: {ex.Message}");
                    // Decide if you want to continue on error or rethrow.  Continuing allows other topics to proceed.
                }
            }
        }

        Console.WriteLine("Consumer group offset reset completed.");
    }

    private static async Task<List<TopicSpecification>> GetTopicConfigurationsAsync(string bootstrapServers)
    {
        var topicSpecifications = new List<TopicSpecification>();

        using (var adminClient =
               new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build())
        {
            try
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(30));
                foreach (var topicMetadata in metadata.Topics)
                {
                    topicSpecifications.Add(new TopicSpecification
                    {
                        Name = topicMetadata.Topic,
                        NumPartitions = topicMetadata.Partitions.Count,
                        ReplicationFactor = (short)Math.Min(metadata.Brokers.Count, 3)
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting topic configurations: {ex.Message}");
                return new List<TopicSpecification>();
            }
        }

        return topicSpecifications;
    }

    private static async Task HandleTopicsAsync(string destinationBootstrapServers, List<TopicSpecification> topicSpecifications,
        bool deleteExisting)
    {
        using var adminClient =
            new AdminClientBuilder(new AdminClientConfig { BootstrapServers = destinationBootstrapServers }).Build();
        foreach (var topicSpecification in topicSpecifications)
        {
            try
            {
                var topicName = topicSpecification.Name;
                var topicMetadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10)).Topics
                    .FirstOrDefault(t => t.Topic == topicName);

                if (topicMetadata != null)
                {
                    if (deleteExisting)
                    {
                        Console.WriteLine($"Deleting topic {topicName}...");
                        try
                        {
                            await adminClient.DeleteTopicsAsync(new List<string> { topicName });
                            Console.WriteLine($"Topic {topicName} deleted.");
                        }
                        catch (DeleteTopicsException e)
                        {
                            Console.WriteLine($"Error deleting topic {topicName}: {e.Results[0].Error.Reason}");
                            continue; // Skip to the next topic
                        }
                    }
                    else
                    {
                        Console.WriteLine($"Topic {topicName} already exists.");
                        continue; // Skip topic creation
                    }
                }

                if (!deleteExisting || topicMetadata == null)
                {
                    // Create the topic if it was deleted or didn't exist
                    Console.WriteLine(
                        $"Creating topic {topicSpecification.Name} with {topicSpecification.NumPartitions} partitions and replication factor {topicSpecification.ReplicationFactor}");

                    var topicSpec = new TopicSpecification
                    {
                        Name = topicSpecification.Name,
                        NumPartitions = topicSpecification.NumPartitions,
                        ReplicationFactor = topicSpecification.ReplicationFactor
                    };

                    try
                    {
                        await adminClient.CreateTopicsAsync(new List<TopicSpecification> { topicSpec });
                        Console.WriteLine($"Topic {topicSpecification.Name} created successfully.");
                        await Task.Delay(2000); // Wait for the topic to be created
                        //await adminClient.CreateTopicsAsync(new List<TopicSpecification> { topicSpec },
                        //    new CreateTopicsOptions(){ValidateOnly = true});
                    }
                    catch (CreateTopicsException e)
                    {
                        Console.WriteLine(
                            $"Error creating topic {topicSpecification.Name}: {e.Results[0].Error.Reason}");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error handling topic {topicSpecification.Name}: {ex.Message}");
            }
        }
    }


    public static async Task SyncConsumerGroupOffsetsAsync(string sourceBootstrapServers,
        string destinationBootstrapServers, string groupId)
    {
        Console.WriteLine($"Starting consumer group offset synchronization for group: {groupId}...");

        try
        {
            // 1. Get topic configurations from the destination cluster.
            var topicConfigurations = await GetTopicConfigurationsAsync(destinationBootstrapServers);

            // 2.  Set all partitions in the destination cluster to the beginning offset (-2)
            var beginningOffsets = new Dictionary<TopicPartition, Offset>();
            using (var adminClient = new AdminClientBuilder(new AdminClientConfig
                       { BootstrapServers = destinationBootstrapServers }).Build())
            {
                foreach (var topicConfiguration in topicConfigurations)
                {
                    for (int partition = 0; partition < topicConfiguration.NumPartitions; partition++)
                    {
                        beginningOffsets.Add(new TopicPartition(topicConfiguration.Name, partition), Offset.Beginning);
                    }

                    try
                    {
                        var resetOptions = new ListConsumerGroupOffsetsOptions
                            { RequestTimeout = TimeSpan.FromSeconds(30) };

                        var offsetsResult = await adminClient.ListConsumerGroupOffsetsAsync(
                            new List<ConsumerGroupTopicPartitions>()
                            {
                                new ConsumerGroupTopicPartitions(groupId, beginningOffsets.Keys.Select(x => x).ToList())
                            }, resetOptions);
                        if (offsetsResult.Any())
                        {
                            var consumerGroupTopicPartitionOffsets = new ConsumerGroupTopicPartitionOffsets(groupId,
                                offsetsResult.First().Partitions
                                    .Select(x => new TopicPartitionOffset(x.TopicPartition, x.Offset)).ToList());
                            await adminClient.AlterConsumerGroupOffsetsAsync(
                                new List<ConsumerGroupTopicPartitionOffsets>() { consumerGroupTopicPartitionOffsets },
                                new AlterConsumerGroupOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(30) });
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Error setting consumer group offsets to beginning: {ex.Message}");
                        // Continue to next topic, or rethrow exception depending on your error handling policy
                    }
                }
            }


            // 3. Get the consumer group offsets from the source cluster.
            var sourceOffsets = await GetConsumerGroupOffsetsAsync(sourceBootstrapServers, groupId);

            // 4. Apply/Set consumer group offsets to the destination cluster.
            await SetConsumerGroupOffsetsAsync(destinationBootstrapServers, groupId, sourceOffsets);

            Console.WriteLine("Consumer group offset synchronization completed.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"An error occurred during consumer group offset synchronization: {ex.Message}");
        }
    }

    private static async Task<Dictionary<TopicPartition, long>> GetConsumerGroupOffsetsAsync(
        string sourceBootstrapServers,
        string groupId)
    {
        var offsets = new Dictionary<TopicPartition, long>();

        using var adminClient =
            new AdminClientBuilder(new AdminClientConfig { BootstrapServers = sourceBootstrapServers }).Build();
        try
        {
            var listOptions = new ListConsumerGroupOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(30) };
            var topicPartitions = new List<ConsumerGroupTopicPartitions>
            {
                new(groupId, null)
            };
            var offsetsResult = await adminClient.ListConsumerGroupOffsetsAsync(topicPartitions, listOptions);

            if (offsetsResult != null && offsetsResult.Any())
            {
                foreach (var topicPartitionOffset in offsetsResult.First().Partitions)
                {
                    if (topicPartitionOffset.Error.IsError)
                    {
                        Console.WriteLine(
                            $"Error fetching offset for topic {topicPartitionOffset.Topic}, partition {topicPartitionOffset.Partition}: {topicPartitionOffset.Error.Reason}");
                        continue;
                    }

                    offsets[new TopicPartition(topicPartitionOffset.Topic, topicPartitionOffset.Partition)] =
                        topicPartitionOffset.Offset;
                }
            }
            else
            {
                Console.WriteLine($"No offsets found for consumer group {groupId}.");
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error getting consumer group offsets: {ex.Message}");
        }

        return offsets;
    }

    private static async Task SetConsumerGroupOffsetsAsync(string bootstrapServers, string groupId,
        Dictionary<TopicPartition, long> offsets)
    {
        using var adminClient =
            new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();

        var groupPartitionOffsets = new List<ConsumerGroupTopicPartitionOffsets>();

        // Check if there are any offsets to set
        if (offsets.Count == 0)
        {
            Console.WriteLine("No offsets to set.");
            return;
        }

        var topicPartitionOffsetList = new List<TopicPartitionOffset>();

        foreach (var kvp in offsets)
        {
            var topicPartition = kvp.Key;
            var offset = kvp.Value;

            topicPartitionOffsetList.Add(new TopicPartitionOffset(topicPartition, offset));
        }

        var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(10));
        Console.WriteLine($"Brokers: {metadata.Brokers.Count}, Topics: {metadata.Topics.Count}");

        var groupResult = await adminClient.DescribeConsumerGroupsAsync([groupId],
            new DescribeConsumerGroupsOptions() { RequestTimeout = TimeSpan.FromSeconds(5) });
        Console.WriteLine(groupResult.ConsumerGroupDescriptions.First().State);

        groupPartitionOffsets.Add(new ConsumerGroupTopicPartitionOffsets(groupId, topicPartitionOffsetList));

        try
        {
            await adminClient.AlterConsumerGroupOffsetsAsync(groupPartitionOffsets,
                new AlterConsumerGroupOffsetsOptions { RequestTimeout = TimeSpan.FromSeconds(5) });
            Console.WriteLine("Successfully set consumer group offsets.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error setting consumer group offsets: {ex.Message}");
        }
    }

    private static async Task ReplicateDataAsync(string sourceBootstrapServers,
        string destinationBootstrapServers, List<TopicSpecification> topicConfigurations)
    {
        Console.WriteLine("Starting data replication...");

        foreach (var topicConfiguration in topicConfigurations)
        {
            await ReplicateTopicDataAsync(sourceBootstrapServers, destinationBootstrapServers,
                topicConfiguration.Name,
                topicConfiguration.NumPartitions);
        }

        Console.WriteLine("Data replication completed.");
    }

    private static async Task ReplicateTopicDataAsync(string sourceBootstrapServers, string destinationBootstrapServers,
        string topicName, int numPartitions)
    {
        Console.WriteLine($"Starting data replication for topic: {topicName}...");

        var tasks = new List<Task>();
        for (int partition = 0; partition < numPartitions; partition++)
        {
            var partition1 = partition;
            tasks.Add(Task.Run(() =>
                ReplicatePartitionDataAsync(sourceBootstrapServers, destinationBootstrapServers, topicName,
                    partition1)));
        }

        await Task.WhenAll(tasks);

        Console.WriteLine($"Data replication completed for topic: {topicName}.");
    }

    private static async Task ReplicatePartitionDataAsync(string sourceBootstrapServers,
        string destinationBootstrapServers, string topicName, int partition)
    {
        Console.WriteLine($"Starting data replication for topic: {topicName}, partition: {partition}...");

        var consumerConfig = new ConsumerConfig
        {
            BootstrapServers = sourceBootstrapServers,
            GroupId = $"temp-{topicName}-{partition}", // Unique group ID for each partition
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false,
            AutoOffsetReset = AutoOffsetReset.Earliest // Start from the beginning
        };

        var producerConfig = new ProducerConfig { BootstrapServers = destinationBootstrapServers };

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var topicPartition = new TopicPartition(topicName, partition);
        consumer.Assign(new List<TopicPartition> { topicPartition }); // Assign the partition

        try
        {
            while (true)
            {
                try
                {
                    var consumeResult = consumer.Consume(TimeSpan.FromMilliseconds(100)); // Adjust timeout as needed

                    if (consumeResult == null || consumeResult.IsPartitionEOF)
                    {
                        // End of partition reached
                        Console.WriteLine($"Reached end of partition {partition} for topic {topicName}");
                        break;
                    }

                    if (consumeResult.Message != null)
                    {
                        try
                        {
                            await producer.ProduceAsync(topicName, consumeResult.Message);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(
                                $"Error producing message to topic {topicName}, partition {partition}: {ex.Message}");
                        }
                    }

                    consumer.Commit(consumeResult);
                }
                catch (ConsumeException e)
                {
                    Console.WriteLine($"Consume error: {e.Error.Reason}");
                    break;
                }
            }

            Console.WriteLine($"Replicated data from topic {topicName}, partition {partition} successfully.");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Error replicating data from topic {topicName}, partition {partition}: {ex.Message}");
        }
        finally
        {
            consumer.Close();
            producer.Flush(TimeSpan.FromSeconds(10));
            producer.Dispose();
        }
    }
}