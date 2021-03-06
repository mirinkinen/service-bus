using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace SimpleProducer
{
    class Program
    {
        static string _connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=7+p4vTqrk9cf2BqY4HpaOkqrad5U8IdnC+vWj+pbC3k=;EntityPath=simple-queue";
        static string _queueName = "simple-queue";

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

                    System.Console.WriteLine($"Sending: {message}");
                    await sender.SendMessageAsync(sbMessage);
                }
            }
        }
    }
}
