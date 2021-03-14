using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Common;
using System.Threading.Tasks;

namespace AsyncSessionConsumer
{
    internal class Program
    {
        private static async Task Main(string[] args)
        {
            var credentials = new DefaultAzureCredential();

            await using var client = new ServiceBusClient(EnvironmentVariable.ServiceBusFqns, credentials);

            var consumerA = Consumer.StartConsumer(client, "ConsumerA");
            var consumerB = Consumer.StartConsumer(client, "ConsumerB");

            Task.WaitAll(consumerA, consumerB);
        }
    }
}