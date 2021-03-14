using Azure.Identity;
using Azure.Messaging.ServiceBus;
using Common;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SessionConsumer
{
    internal class Program
    {
        private static readonly Random _random = new(Guid.NewGuid().GetHashCode());

        private static readonly MessageHandlingOrderValidator _messageHandlingStatistics = new();

        private static async Task Main(string[] args)
        {
            var credentials = new DefaultAzureCredential();

            await using var client = new ServiceBusClient(EnvironmentVariable.ServiceBusFqns, credentials);

            var consumerA = ConsumeMessages(client, "ConsumerA");
            var consumerB = ConsumeMessages(client, "ConsumerB");

            Task.WaitAll(consumerA, consumerB);
        }

        private static async Task ConsumeMessages(ServiceBusClient client, string consumerName)
        {
            while (true)
            {
                await using ServiceBusSessionReceiver receiver = await client.AcceptNextSessionAsync(EnvironmentVariable.SessionQueue);
                ConsoleHelper.WriteInfo($"{consumerName}: Locked to session {receiver.SessionId}");

                ServiceBusReceivedMessage message;
                while ((message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2))) != null)
                {
                    try
                    {
                        var sessionState = await GetSessionState(receiver);

                        Console.WriteLine($"{consumerName}: {message.Body}, SessionID: {message.SessionId}, SeqNum: {message.SequenceNumber}");

                        if (sessionState.IsStateComplete("A"))
                        {
                            ConsoleHelper.WriteInfo($"{consumerName}: State A already complete!");
                        }
                        else
                        {
                            // Execute work A. This must be done only once per message!
                            DoWork();

                            // Mark that phase A was completed.
                            sessionState.MarkStateComplete("A");
                            await receiver.SetSessionStateAsync(new BinaryData(sessionState));
                        }

                        // Execute work B.
                        DoWork();


                        // Message handled!
                        await receiver.SetSessionStateAsync(null);
                        _messageHandlingStatistics.ValidateOrder(message.SessionId, message.SequenceNumber);
                        await receiver.CompleteMessageAsync(message);
                    }
                    catch (Exception ex)
                    {
                        ConsoleHelper.WriteError($"{consumerName}: {ex.Message}");
                        await receiver.AbandonMessageAsync(message);
                    }
                }

                ConsoleHelper.WriteInfo($"{consumerName}: Session {receiver.SessionId} is empty");
            }
        }

        private static async Task<SessionState> GetSessionState(ServiceBusSessionReceiver receiver)
        {
            var binaryState = await receiver.GetSessionStateAsync();
            return binaryState != null ? binaryState.ToObjectFromJson<SessionState>() : new SessionState();
        }

        private static void DoWork()
        {
            if (_random.Next(1, 101) >= 90)
            {
                throw new InvalidOperationException($"Some error occurred while working.");
            }
        }
    }

    public class SessionState
    {
        public List<string> StatesCompleted { get; set; } = new List<string>();

        public void MarkStateComplete(string state)
        {
            StatesCompleted.Add(state);
        }

        public bool IsStateComplete(string state)
        {
            return StatesCompleted.Contains(state);
        }
    }
}