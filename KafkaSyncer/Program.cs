namespace KafkaSyncer;

class Program
{
    private static async Task Main(string[] args)
    {
        Console.WriteLine("Kafka Metadata Syncer");

        // Configuration
        const string sourceBootstrapServers = "localhost:9092";
        const string destinationBootstrapServers = "localhost:9094";
        const string topicName = "my-topic"; // Replace with your topic name
        const string groupId = "my-group"; // Replace with your group ID

        // Synchronization process
        Console.WriteLine("Starting Kafka metadata synchronization...");
        //await KafkaSync.SyncKafkaAsync(sourceBootstrapServers, destinationBootstrapServers, groupId);
        Console.WriteLine("Kafka metadata synchronization completed.");

        // Verification process
        Console.WriteLine("Starting Kafka verification...");
        await KafkaSync.VerifySync(sourceBootstrapServers, destinationBootstrapServers, topicName, groupId);
        Console.WriteLine("Kafka verification completed.");
    }
}