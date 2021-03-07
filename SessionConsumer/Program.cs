using Azure.Messaging.ServiceBus;
using Common;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SessionConsumer
{
    internal class Program
    {
        private static readonly string _connectionString = "Endpoint=sb://sb-111.servicebus.windows.net/;SharedAccessKeyName=listen;SharedAccessKey=c6/9qt6zjnH5XA/Xo4NQS4pIB0QVjv3WpoJ0vdLZetU=;EntityPath=partition-session-queue";
        private static readonly string _queueName = "partition-session-queue";

        private static readonly Random _random = new(Guid.NewGuid().GetHashCode());

        private static readonly Dictionary<string, long> _sessionSequenceNumbers = new Dictionary<string, long>();

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
            await using var client = new ServiceBusClient(_connectionString);
            while (true)
            {
                ServiceBusSessionReceiver receiver = null;
                try
                {
                    receiver = await client.AcceptNextSessionAsync(_queueName);

                    Console.WriteLine($"Receiver locked to session {receiver.SessionId}");

                    while (true)
                    {
                        var message = await receiver.ReceiveMessageAsync(TimeSpan.FromSeconds(2));

                        // If no messages, keep polling..
                        if (message == null)
                        {
                            Console.WriteLine($"Queue empty for session {receiver.SessionId}");
                            break;
                        }

                        long sequenceNumberDiff = UpdateSequenceNumber(message);

                        Console.WriteLine($"{consumerName}: {message.Body}, SessionID: {message.SessionId}, SeqNum: {message.SequenceNumber}, Diff: {sequenceNumberDiff}");

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
                }
                catch (Exception ex)
                {
                    ConsoleHelper.WriteError(ex.Message);
                    await receiver.CloseAsync();
                }
            }
        }

        private static long UpdateSequenceNumber(ServiceBusReceivedMessage message)
        {
            if (!_sessionSequenceNumbers.TryGetValue(message.SessionId, out long sequenceNumber))
            {
                _sessionSequenceNumbers.Add(message.SessionId, message.SequenceNumber);
                return 0;
            }
            else
            {
                if (sequenceNumber > message.SequenceNumber)
                {
                    ConsoleHelper.WriteWarning("OUT OF ORDER!");
                }
                else
                {
                    _sessionSequenceNumbers.Remove(message.SessionId);
                    _sessionSequenceNumbers.Add(message.SessionId, message.SequenceNumber);
                }

                return message.SequenceNumber - sequenceNumber;
            }
        }
    }
}