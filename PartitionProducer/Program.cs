using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Common;
using System;
using System.Threading.Tasks;

namespace PartitionProducer
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            await ProduceMessages("A");
            await ProduceMessages("B");
            await ProduceMessages("C");
        }

        private static async Task ProduceMessages(string partitionKey)
        {
            var credentials = new DefaultAzureCredential();

            await using var client = new ServiceBusClient(EnvironmentVariable.ServiceBusFqns, credentials);
            await using ServiceBusSender sender = client.CreateSender(EnvironmentVariable.PartitionedQueue);

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