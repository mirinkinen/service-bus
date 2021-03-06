using System;
using System.Threading;
using System.Threading.Tasks;
using Azure.Messaging.ServiceBus;

namespace producer
{
    class Program
    {
        static string connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=send;SharedAccessKey=7+p4vTqrk9cf2BqY4HpaOkqrad5U8IdnC+vWj+pbC3k=;EntityPath=simple-queue";
        static string queueName = "simple-queue";

        static void Main(string[] args)
        {
            try
            {
                var cancellationTokenSource = new CancellationTokenSource();
                var task = new Task(() => ProduceMessages(cancellationTokenSource.Token));

                System.Console.WriteLine("Press [enter] to stop producing messages.");
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

        private static async Task ProduceMessages(CancellationToken cancellationToken)
        {
            try
            {
                await using (var client = new ServiceBusClient(connectionString))
                {
                    ServiceBusSender sender = client.CreateSender(queueName);

                    for (int i = 0; i < 20; i++)
                    {
                        if (cancellationToken.IsCancellationRequested)
                        {
                            return;
                        }

                        var message = $"Message {i}";
                        System.Console.WriteLine($"Sending: {message}");
                        await sender.SendMessageAsync(new ServiceBusMessage(message), cancellationToken);
                    }
                }
            }
            catch (Exception ex)
            {
                System.Console.WriteLine($"Error: {ex.Message}");
            }
            finally
            {
                System.Console.WriteLine("Message producing stopped.");
            }
        }
    }
}
