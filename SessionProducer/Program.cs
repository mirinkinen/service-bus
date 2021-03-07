using Azure.Messaging.ServiceBus;
using System;
using System.Threading.Tasks;

namespace SessionProducer
{
    internal class Program
    {
        private static readonly string _connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=jEXa6J+QEpQASvXXzHGxN7EnFW8aVTvdFyMIsgygx58=;EntityPath=partition-session-queue";
        private static readonly string _queueName = "partition-session-queue";

        private static async Task Main(string[] args)
        {
            await ProduceMessages("A");
            await ProduceMessages("B");
            await ProduceMessages("C");
        }

        private static async Task ProduceMessages(string partitionKey)
        {
            await using var client = new ServiceBusClient(_connectionString);
            await using ServiceBusSender sender = client.CreateSender(_queueName);

            for (int i = 1; i <= 5; i++)
            {
                var messsageBody = $"Message {i}";
                var message = new ServiceBusMessage(messsageBody);
                message.SessionId = partitionKey;

                Console.WriteLine($"Sending: {messsageBody} SessionId: {message.SessionId}");
                await sender.SendMessageAsync(message);
            }
        }
    }
}