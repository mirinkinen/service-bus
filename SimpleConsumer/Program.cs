using Azure.Messaging.ServiceBus;
using Common;
using System;
using System.Threading.Tasks;

namespace SimpleConsumer
{
    internal class Program
    {
        private static string connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=0yh7ESr7/jbppmc/pXV9coVWl8jPFQxhyQ831A9A4dc=;EntityPath=simple-queue";
        private static string _queueName = "simple-queue";

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
            await using var client = new ServiceBusClient(connectionString);
            await using var receiver = client.CreateReceiver(_queueName);

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