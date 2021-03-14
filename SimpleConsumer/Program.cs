using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Common;
using System;
using System.Threading.Tasks;

namespace SimpleConsumer
{
    internal class Program
    {
        private static readonly MessageHandlingOrderValidator _orderValidator = new();

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
            await using var receiver = client.CreateReceiver(EnvironmentVariable.SimpleQueue);

            ServiceBusReceivedMessage message = null;
            while ((message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2))) != null)
            {
                Console.WriteLine($"{consumerName} consuming: {message.Body}, SeqNumber: {message.SequenceNumber}");

                _orderValidator.ValidateOrder(message.SequenceNumber);

                await receiver.CompleteMessageAsync(message);
            }
        }
    }
}