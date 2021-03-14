using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Common;
using System;
using System.Threading.Tasks;

namespace SimpleProducer
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            await ProduceMessages();
        }

        private static async Task ProduceMessages()
        {
            var credentials = new DefaultAzureCredential();

            await using var client = new ServiceBusClient(EnvironmentVariable.ServiceBusFqns, credentials);
            await using ServiceBusSender sender = client.CreateSender(EnvironmentVariable.SimpleQueue);

            for (int i = 1; i <= 50; i++)
            {
                var messsageBody = $"Message {i}";
                var message = new ServiceBusMessage(messsageBody);

                Console.WriteLine($"Sending: {messsageBody}");
                await sender.SendMessageAsync(message);
            }
        }
    }
}