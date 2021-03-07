using Azure.Messaging.ServiceBus;
using Common;
using System;
using System.Threading.Tasks;

namespace PartitionConsumer
{
    internal class Program
    {
        private static readonly string _connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=RgnQcqIEjXDvI48VrD8ErJzkn+tCyxFAIQQHlDrjVto=;EntityPath=partition-queue";
        private static readonly string _queueName = "partition-queue";

        private static readonly MessageHandlingOrderValidator _messageHandlingStatistics = new();

        private static async Task Main(string[] args)
        {
            await ConsumeMessages("ConsumerA");

            //var consumerA = ConsumeMessages("ConsumerA");
            //var consumerB = ConsumeMessages("ConsumerB");
            //Task.WaitAll(consumerA, consumerB);
        }

        private static async Task ConsumeMessages(string consumerName)
        {
            await using var client = new ServiceBusClient(_connectionString);
            ServiceBusReceiver receiver = client.CreateReceiver(_queueName);

            ServiceBusReceivedMessage message = null;
            while ((message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2))) != null)
            {
                Console.WriteLine($"{consumerName}: {message.Body}, PartitionKey: {message.PartitionKey}, SeqNum: {message.SequenceNumber}");

                _messageHandlingStatistics.ValidateOrder(message.PartitionKey, message.SequenceNumber);

                await receiver.CompleteMessageAsync(message);
            }
        }
    }
}