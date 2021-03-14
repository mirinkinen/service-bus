using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Common;
using System;
using System.Threading.Tasks;

namespace SessionProducer
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var credentials = new DefaultAzureCredential();

            await using var client = new ServiceBusClient(EnvironmentVariable.ServiceBusFqns, credentials);

            await ProduceMessages(client, "A");
            await ProduceMessages(client, "B");
            await ProduceMessages(client, "C");
        }

        private static async Task ProduceMessages(ServiceBusClient client, string sessionId)
        {
            await using ServiceBusSender sender = client.CreateSender(EnvironmentVariable.SessionQueue);

            for (int i = 1; i <= 10; i++)
            {
                var messsageBody = $"Message {i}";
                var message = new ServiceBusMessage(messsageBody);
                message.SessionId = sessionId;

                Console.WriteLine($"Sending: {messsageBody} SessionId: {message.SessionId}");
                await sender.SendMessageAsync(message);
            }
        }
    }
}