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

        private static readonly Random _random = new(Guid.NewGuid().GetHashCode());

        private static async Task Main(string[] args)
        {
            try
            {
                if (args.Length != 2)
                {
                    Console.WriteLine("Give consumer name and delay between reads in milliseconds: consumer.exe consumerA 500");
                    return;
                }

                var consumerName = args[0].ToString();
                var readDelay = TimeSpan.FromMilliseconds(Convert.ToInt32(args[1].ToString()));

                await ConsumeMessages(consumerName, readDelay);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static async Task ConsumeMessages(string consumerName, TimeSpan readDelay)
        {
            await using var client = new ServiceBusClient(connectionString);
            await using var receiver = client.CreateReceiver(_queueName);

            Console.WriteLine("Ready to consume");

            long sequenceNumber = 0;

            while (true)
            {
                try
                {
                    var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));

                    // If no messages, keep polling..
                    if (message == null) continue;

                    var sequenceNumberDiff = message.SequenceNumber - sequenceNumber;

                    if (sequenceNumber < message.SequenceNumber)
                    {
                        sequenceNumber = message.SequenceNumber;
                    }

                    Console.WriteLine($"{consumerName} consuming: {message.Body}, SeqNumber: {message.SequenceNumber}, Diff: {sequenceNumberDiff}");

                    if (sequenceNumberDiff < 0)
                    {
                        ConsoleHelper.WriteWarning("OUT OF ORDER!");
                    }

                    // Simulate work...
                    if (readDelay != TimeSpan.Zero)
                    {
                        await Task.Delay(readDelay);
                    }

                    if (_random.Next(1, 101) >= 95)
                    {
                        throw new InvalidOperationException($"Failed to process item {message.SequenceNumber}");
                    }

                    await receiver.CompleteMessageAsync(message);
                }
                catch (Exception ex)
                {
                    ConsoleHelper.WriteError(ex.Message);
                }
            }
        }
    }
}