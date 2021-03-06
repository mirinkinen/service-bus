using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace SessionConsumer
{
    class Program
    {
        static string connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=c6/9qt6zjnH5XA/Xo4NQS4pIB0QVjv3WpoJ0vdLZetU=;EntityPath=partition-session-queue";
        static string queueName = "partition-session-queue";

        static async Task Main(string[] args)
        {
            try
            {
                if (args.Length != 2)
                {
                    System.Console.WriteLine("Give consumer name and delay between reads in milliseconds: consumer.exe consumerA 500");
                    return;
                }

                var consumerName = args[0].ToString();
                var readDelay = TimeSpan.FromMilliseconds(Convert.ToInt32(args[1].ToString()));

                await ConsumeMessages(consumerName, readDelay);

            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static async Task ConsumeMessages(string consumerName, TimeSpan readDelay)
        {
            await using (var client = new ServiceBusClient(connectionString))
            {
                while (true)
                {
                    var receiver = await client.AcceptNextSessionAsync(queueName);
                    System.Console.WriteLine("Ready to consume session");

                    while (true)
                    {
                        var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
                        if (message == null)
                        {
                            System.Console.WriteLine("Queue is empty for session.");
                            break;
                        }

                        System.Console.WriteLine($"Consuming: {message.Body.ToString()}, SessionID: {message.SessionId}, PartitionKey: {message.PartitionKey}");

                        // Simulate work...
                        await Task.Delay(readDelay);

                        await receiver.CompleteMessageAsync(message);
                    }
                }
            }
        }
    }
}

