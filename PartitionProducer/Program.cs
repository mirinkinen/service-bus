using Azure.Messaging.ServiceBus;
using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;

namespace PartitionProducer
{
    internal class Program
    {
        private static readonly string _connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=pFwub4jvfyvOxSSSr50BWsfD/dypad0b5bWbr5db5/Q=;EntityPath=partition-queue";
        private static readonly string _queueName = "partition-queue";

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

            await CreateMessagesForPartition(sender, messageCount, "A");
            await CreateMessagesForPartition(sender, messageCount, "B");
        }

        private static async Task CreateMessagesForPartition(ServiceBusSender sender, int messageCount, string partitionKey)
        {
            CommittableTransaction committableTransaction = new();

            for (int i = 0; i < messageCount; i++)
            {
                using TransactionScope ts = new(committableTransaction, TransactionScopeAsyncFlowOption.Enabled);
                var messageNumber = Interlocked.Increment(ref _messageNumber);

                var messageBody = $"Message {messageNumber}";
                var message = new ServiceBusMessage(messageBody)
                {
                    PartitionKey = partitionKey
                };

                Console.WriteLine($"Producing: {messageBody}, PartitionKey: {message.PartitionKey}");
                await sender.SendMessageAsync(message);

                ts.Complete();
            }

            committableTransaction.Commit();
        }

        private static string GetRandomPartitionKey()
        {
            var sessionIds = "AB";
            return sessionIds.ElementAt(_random.Next(0, sessionIds.Length)).ToString();
        }
    }
}