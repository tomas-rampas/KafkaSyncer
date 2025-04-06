namespace KafkaSyncerConsumer;

class Program
{
    private static async Task Main(string[] args)
    {
        Console.WriteLine("Kafka Consumer");

        string kafkaTopic = "my-topic"; // Replace with your topic name
        string kafkaBootstrapServers = "localhost:9092"; // Replace with your Kafka broker address
        string kafkaGroupId = "my-group"; // Replace with your consumer group ID

        var consumer = new KafkaMessageConsumer(kafkaBootstrapServers, kafkaTopic, kafkaGroupId);

        // Create a cancellation token source to allow the consumer to be stopped
        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (sender, e) =>
        {
            e.Cancel = true; // Prevent the application from exiting immediately
            cts.Cancel();
        };

        Console.WriteLine("Press Ctrl+C to stop the consumer.");

       var result = await consumer.ReadAllMessagesFromBeginningAsync<string, string>(kafkaTopic, kafkaGroupId, cts.Token);
       await consumer.ConsumeMessagesAsync(cts.Token);


        Console.WriteLine("Consumer stopped.");
    }
}