using Azure.Messaging.ServiceBus;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace SessionProducer
{
    internal class Program
    {
        private static readonly string _connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=jEXa6J+QEpQASvXXzHGxN7EnFW8aVTvdFyMIsgygx58=;EntityPath=partition-session-queue";
        private static readonly string _queueName = "partition-session-queue";

        private static readonly Random _random = new(Guid.NewGuid().GetHashCode());
        private static int _messageNumber = 0;

        private static async Task Main(string[] args)
        {
            try
            {
                var messageCount = args.Length == 1 ? Convert.ToInt32(args[0]) : 100;

                await ProduceMessages(messageCount);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static async Task ProduceMessages(int messageCount)
        {
            await using var client = new ServiceBusClient(_connectionString);
            await using ServiceBusSender sender = client.CreateSender(_queueName);

            for (int i = 0; i < messageCount; i++)
            {
                var messageNumber = Interlocked.Increment(ref _messageNumber);

                var messageBody = $"Message {messageNumber}";
                var message = new ServiceBusMessage(messageBody)
                {
                    SessionId = GetRandomSessionId()
                };

                Console.WriteLine($"Producing: {messageBody}, SessionID: {message.SessionId}, PartitionKey: {message.PartitionKey}");
                await sender.SendMessageAsync(message);
            }
        }

        private static string GetRandomSessionId()
        {
            var sessionIds = "ABC";
            return sessionIds.ElementAt(_random.Next(0, sessionIds.Length - 1)).ToString();

        }
    }
}