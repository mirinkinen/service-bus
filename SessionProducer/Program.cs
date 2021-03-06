using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace SessionProducer
{
    class Program
    {
        static string _connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=jEXa6J+QEpQASvXXzHGxN7EnFW8aVTvdFyMIsgygx58=;EntityPath=partition-session-queue";
        static string _queueName = "partition-session-queue";

        static Random _random = new Random(Guid.NewGuid().GetHashCode());

        static async Task Main(string[] args)
        {
            try
            {
                await ProduceMessages();
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static async Task ProduceMessages()
        {
            await using (var client = new ServiceBusClient(_connectionString))
            {
                ServiceBusSender sender = client.CreateSender(_queueName);

                for (int i = 0; i < 20; i++)
                {
                    var message = $"Message {i}";
                    var sbMessage = new ServiceBusMessage(message);
                    sbMessage.SessionId = $"Session {GetRandomLetter()}";

                    System.Console.WriteLine($"Producing: {message}, SessionID: {sbMessage.SessionId}, PartitionKey: {sbMessage.PartitionKey}");
                    await sender.SendMessageAsync(sbMessage);
                }
            }
        }

        private static char GetRandomLetter()
        {
            return "ABCDE".ElementAt(_random.Next(0, 4));
        }
    }
}
