using Confluent.Kafka;
using System.Text.Json;

namespace KafkaSyncerProducer;

class KafkaMessageGenerator
{
    private static readonly Random random = new();

    public static async Task ProduceMessagesAsync(string bootstrapServers, string topic, int messageCount)
    {
        var config = new ProducerConfig { BootstrapServers = bootstrapServers };

        using (var producer = new ProducerBuilder<Null, string>(config).Build())
        {
            for (int i = 0; i < messageCount; i++)
            {
                var message = GenerateRandomMessage();
                try
                {
                    var deliveryReport = await producer.ProduceAsync(topic, new Message<Null, string> { Value = message });
                    Console.WriteLine($"Delivered message to: {deliveryReport.TopicPartitionOffset}");
                }
                catch (ProduceException<Null, string> e)
                {
                    Console.WriteLine($"Failed to deliver message: {e.Error.Reason}");
                }
            }

            // Wait for any outstanding messages to be delivered.
            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }

    private static string GenerateRandomMessage()
    {
        var message = new
        {
            id = random.Next(),
            data = RandomString(random.Next(1, 51)) // Random string with length between 1 and 50
        };

        return JsonSerializer.Serialize(message);
    }

    private static string RandomString(int length)
    {
        const string chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        return new string(Enumerable.Repeat(chars, length)
            .Select(s => s[random.Next(s.Length)]).ToArray());
    }
}