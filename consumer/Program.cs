using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace consumer
{
    class Program
    {
        static string connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=0yh7ESr7/jbppmc/pXV9coVWl8jPFQxhyQ831A9A4dc=;EntityPath=simple-queue";
        static string queueName = "simple-queue";

        static void Main(string[] args)
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

                var cancellationTokenSource = new CancellationTokenSource();
                var task = new Task(() => ConsumeMessages(consumerName, readDelay, cancellationTokenSource.Token));

                System.Console.WriteLine("Press [enter] to stop consuming messages.");
                task.Start();
                System.Console.ReadLine();
                System.Console.WriteLine("Cancellation requested.");
                cancellationTokenSource.Cancel();
                task.Wait();
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"Error: {ex.Message}");
            }
        }

        private static async Task ConsumeMessages(string consumerName, TimeSpan readDelay, CancellationToken cancellationToken)
        {
            try
            {
                await using (var client = new ServiceBusClient(connectionString))
                {
                    var options = new ServiceBusReceiverOptions
                    {
                        ReceiveMode = ServiceBusReceiveMode.PeekLock
                    };

                    ServiceBusReceiver receiver = client.CreateReceiver(queueName, options);
                    System.Console.WriteLine("Ready to consume");

                    while (true)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }

                        var message = await receiver.ReceiveMessageAsync();
                        System.Console.WriteLine($"{consumerName} consuming: {message.Body.ToString()}");

                        // Simulate work...
                        await Task.Delay(readDelay);

                        await receiver.CompleteMessageAsync(message);
                    }
                }
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"Error: {ex.Message}");
            }
            finally
            {
                System.Console.WriteLine("Message consuming stopped.");
            }
        }
    }
}
