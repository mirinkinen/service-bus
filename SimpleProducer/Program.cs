using Azure.Messaging.ServiceBus;
using System;
using System.Threading.Tasks;

namespace SimpleProducer
{
    internal class Program
    {
        private static string _connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=7+p4vTqrk9cf2BqY4HpaOkqrad5U8IdnC+vWj+pbC3k=;EntityPath=simple-queue";
        private static string _queueName = "simple-queue";

        private static async Task Main(string[] args)
        {
            await ProduceMessages();
        }

        private static async Task ProduceMessages()
        {
            await using var client = new ServiceBusClient(_connectionString);
            await using ServiceBusSender sender = client.CreateSender(_queueName);

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