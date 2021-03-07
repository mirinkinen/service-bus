using Azure.Messaging.ServiceBus;
using System;
using System.Threading.Tasks;

namespace PartitionProducer
{
    internal class Program
    {
        private static readonly string _connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=pFwub4jvfyvOxSSSr50BWsfD/dypad0b5bWbr5db5/Q=;EntityPath=partition-queue";
        private static readonly string _queueName = "partition-queue";

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

            for (int i = 1; i <= 10; i++)
            {
                var messsageBody = $"Message {i}";
                var message = new ServiceBusMessage(messsageBody);
                message.PartitionKey = partitionKey;

                Console.WriteLine($"Sending: {messsageBody} PartitionKey: {message.PartitionKey}");
                await sender.SendMessageAsync(message);
            }
        }
    }
}