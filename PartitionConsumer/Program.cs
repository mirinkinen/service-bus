using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Common;
using System;
using System.Threading.Tasks;

namespace PartitionConsumer
{
    internal class Program
    {
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
            var credentials = new DefaultAzureCredential();

            await using var client = new ServiceBusClient(EnvironmentVariable.ServiceBusFqns, credentials);
            ServiceBusReceiver receiver = client.CreateReceiver(EnvironmentVariable.PartitionedQueue);

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