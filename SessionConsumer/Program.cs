using Azure.Messaging.ServiceBus;
using Common;
using System;
using System.Threading.Tasks;

namespace SessionConsumer
{
    internal class Program
    {
        private static readonly string _connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=c6/9qt6zjnH5XA/Xo4NQS4pIB0QVjv3WpoJ0vdLZetU=;EntityPath=partition-session-queue";
        private static readonly string _queueName = "partition-session-queue";

        private static readonly Random _random = new(Guid.NewGuid().GetHashCode());

        private static readonly MessageHandlingOrderValidator _messageHandlingStatistics = new();

        private static void Main(string[] args)
        {
            var consumerA = ConsumeMessages("ConsumerA");
            var consumerB = ConsumeMessages("ConsumerB");
            Task.WaitAll(consumerA, consumerB);
        }

        private static async Task ConsumeMessages(string consumerName)
        {
            await using var client = new ServiceBusClient(_connectionString);
            while (true)
            {
                ServiceBusSessionReceiver receiver = null;
                try
                {
                    receiver = await client.AcceptNextSessionAsync(_queueName);

                    Console.WriteLine($"{consumerName} locked to session {receiver.SessionId}");

                    ServiceBusReceivedMessage message = null;
                    while ((message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2))) != null)
                    {
                        Console.WriteLine($"{consumerName}: {message.Body}, SessionID: {message.SessionId}, SeqNum: {message.SequenceNumber}");

                        //ThrowTransientError(message);

                        _messageHandlingStatistics.ValidateOrder(message.SessionId, message.SequenceNumber);

                        await receiver.CompleteMessageAsync(message);
                    }

                    Console.WriteLine($"{consumerName}: Session {receiver.SessionId} is empty");
                }
                catch (Exception ex)
                {
                    ConsoleHelper.WriteError(ex.Message);
                    await receiver.CloseAsync();
                }
            }
        }

        private static void ThrowTransientError(ServiceBusReceivedMessage message)
        {
            if (_random.Next(1, 101) >= 80)
            {
                throw new InvalidOperationException($"Failed to process item {message.SequenceNumber}");
            }
        }
    }
}