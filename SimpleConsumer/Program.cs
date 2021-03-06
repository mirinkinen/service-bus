using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace SimpleConsumer
{
    class Program
    {
        static string connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=0yh7ESr7/jbppmc/pXV9coVWl8jPFQxhyQ831A9A4dc=;EntityPath=simple-queue";
        static string _queueName = "simple-queue";

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
                var options = new ServiceBusReceiverOptions
                {
                    ReceiveMode = ServiceBusReceiveMode.PeekLock,
                };

                var receiver = client.CreateReceiver(_queueName);
                System.Console.WriteLine("Ready to consume");

                while (true)
                {
                    var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(1));
                    if (message != null)
                    {
                        System.Console.WriteLine($"{consumerName} consuming: {message.Body.ToString()}");
                    }

                    // Simulate work...
                    await Task.Delay(readDelay);

                    if (message != null)
                    {
                        await receiver.CompleteMessageAsync(message);
                    }
                }
            }
        }
    }
}
