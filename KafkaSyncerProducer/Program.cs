namespace KafkaSyncerProducer;

class Program
{
    static async Task  Main(string[] args)
    {
        Console.WriteLine("Kafka Producer");

        // Kafka Producer
        string kafkaTopic = "my-topic"; 
        string kafkaBootstrapServers = "localhost:9092"; 
        int messageCount = 3;

        Console.WriteLine($"Producing {messageCount} messages to topic {kafkaTopic}...");
        await KafkaMessageGenerator.ProduceMessagesAsync(kafkaBootstrapServers, kafkaTopic, messageCount);
        Console.WriteLine("Message production completed.");
    }
}
