using Azure.Messaging.ServiceBus;
using Common;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace AsyncSessionConsumer
{
    internal class Consumer
    {
        private static readonly Random _random = new(Guid.NewGuid().GetHashCode());

        private readonly ServiceBusClient _client;
        private readonly string _consumerName;

        private readonly MessageHandlingOrderValidator _messageHandlingStatistics = new();
        private CancellationTokenSource _cancellationTokenSource;

        public Consumer(ServiceBusClient client, string consumerName)
        {
            _client = client;
            _consumerName = consumerName;
        }

        public static Task StartConsumer(ServiceBusClient client, string consumerName)
        {
            var consumer = new Consumer(client, consumerName);
            return consumer.Start();
        }

        private async Task Start()
        {
            var options = new ServiceBusSessionProcessorOptions()
            {
                MaxConcurrentSessions = 2
            };

            var sessionProcessor = _client.CreateSessionProcessor(EnvironmentVariable.SessionQueue, options);
            sessionProcessor.ProcessMessageAsync += SessionProcessor_ProcessMessageAsync;
            sessionProcessor.SessionInitializingAsync += SessionProcessor_SessionInitializingAsync;
            sessionProcessor.SessionClosingAsync += SessionProcessor_SessionClosingAsync;
            sessionProcessor.ProcessErrorAsync += SessionProcessor_ProcessErrorAsync;

            await sessionProcessor.StartProcessingAsync();

            try
            {
                _cancellationTokenSource = new CancellationTokenSource();
                await Task.Delay(Timeout.InfiniteTimeSpan, _cancellationTokenSource.Token);
            }
            catch (TaskCanceledException)
            {
                ConsoleHelper.WriteInfo($"{_consumerName} canceled.");
            }
            finally
            {
                await sessionProcessor.StopProcessingAsync();
            }
        }

        private static Task SessionProcessor_ProcessErrorAsync(ProcessErrorEventArgs arg)
        {
            ConsoleHelper.WriteError($"Error: {arg.Exception.Message}. InnerException: {arg.Exception.InnerException?.Message}");
            return Task.CompletedTask;
        }

        private Task SessionProcessor_SessionClosingAsync(ProcessSessionEventArgs arg)
        {
            ConsoleHelper.WriteInfo($"{_consumerName}: Session {arg.SessionId} is empty");
            return Task.CompletedTask;
        }

        private Task SessionProcessor_SessionInitializingAsync(ProcessSessionEventArgs arg)
        {
            _cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(5));

            ConsoleHelper.WriteInfo($"{_consumerName}: Locked to session {arg.SessionId}");
            return Task.CompletedTask;
        }

        private async Task SessionProcessor_ProcessMessageAsync(ProcessSessionMessageEventArgs arg)
        {
            try
            {
                // Add 5 seconds more time to live.
                _cancellationTokenSource.CancelAfter(TimeSpan.FromSeconds(5));

                var message = arg.Message;
                var sessionState = await GetSessionState(arg);

                Console.WriteLine($"{_consumerName}: {message.Body}, SessionID: {message.SessionId}, SeqNum: {message.SequenceNumber}");

                if (sessionState.IsStateComplete("A"))
                {
                    ConsoleHelper.WriteInfo($"{_consumerName}: State A already complete!");
                }
                else
                {
                    // Execute work A. This must be done only once per message!
                    DoWork();

                    // Mark that phase A was completed.
                    sessionState.MarkStateComplete("A");
                    await arg.SetSessionStateAsync(new BinaryData(sessionState));
                }

                // Execute work B.
                DoWork();

                // Message handled!
                await arg.SetSessionStateAsync(null);
                _messageHandlingStatistics.ValidateOrder(message.SessionId, message.SequenceNumber);
                await arg.CompleteMessageAsync(message);
            }
            catch (Exception ex)
            {
                ConsoleHelper.WriteError($"{_consumerName}: {ex.Message}");
                await arg.AbandonMessageAsync(arg.Message);
            }
        }

        private static async Task<SessionState> GetSessionState(ProcessSessionMessageEventArgs arg)
        {
            var binaryState = await arg.GetSessionStateAsync();
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