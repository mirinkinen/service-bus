using Azure.Messaging.ServiceBus;
using Common;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace SessionConsumer
{
    internal class Program
    {
        private static readonly string _connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=c6/9qt6zjnH5XA/Xo4NQS4pIB0QVjv3WpoJ0vdLZetU=;EntityPath=partition-session-queue";
        private static readonly string _queueName = "partition-session-queue";

        private static readonly Random _random = new(Guid.NewGuid().GetHashCode());

        private static readonly MessageHandlingOrderValidator _messageHandlingStatistics = new();

        private static void Main(string[] args)
        {
            var consumerA = ConsumeMessages("ConsumerA");
            var consumerB = ConsumeMessages("ConsumerB");
            Task.WaitAll(consumerA, consumerB);
        }

        private static async Task ConsumeMessages(string consumerName)
        {
            await using var client = new ServiceBusClient(_connectionString);
            while (true)
            {
                ServiceBusSessionReceiver receiver = await client.AcceptNextSessionAsync(_queueName);

                ConsoleHelper.WriteInfo($"{consumerName} locked to session {receiver.SessionId}");

                ServiceBusReceivedMessage message;
                while ((message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2))) != null)
                {
                    try
                    {
                        var sessionState = await GetSessionState(receiver);

                        Console.WriteLine($"{consumerName}: {message.Body}, SessionID: {message.SessionId}, SeqNum: {message.SequenceNumber}");

                        if (sessionState.IsStateComplete("A"))
                        {
                            ConsoleHelper.WriteInfo("State A already complete!");
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
                        _messageHandlingStatistics.ValidateOrder(message.SessionId, message.SequenceNumber);
                        await receiver.CompleteMessageAsync(message);
                    }
                    catch (Exception ex)
                    {
                        ConsoleHelper.WriteError(ex.Message);
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
            if (_random.Next(1, 101) >= 80)
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