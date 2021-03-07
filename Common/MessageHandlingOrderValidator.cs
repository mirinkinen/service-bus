using System.Collections.Generic;

namespace Common
{
    public class MessageHandlingOrderValidator
    {
        private long _maxSequenceNumber;
        private readonly Dictionary<string, long> _partitionSequenceNumbers = new();

        public void ValidateOrder(long sequenceNumber)
        {
            lock (this)
            {
                if (sequenceNumber > _maxSequenceNumber)
                {
                    _maxSequenceNumber = sequenceNumber;
                }
                else
                {
                    ConsoleHelper.WriteError($"{sequenceNumber} is handled in wrong order");
                }
            }
        }

        public void ValidateOrder(string partitionKey, long sequenceNumber)
        {
            lock (this)
            {
                if (!_partitionSequenceNumbers.TryGetValue(partitionKey, out long maxSequenceNumber))
                {
                    _partitionSequenceNumbers.Add(partitionKey, sequenceNumber);
                }
                else
                {
                    if (maxSequenceNumber > sequenceNumber)
                    {
                        ConsoleHelper.WriteError("OUT OF ORDER!");
                    }
                    else
                    {
                        _partitionSequenceNumbers[partitionKey] = sequenceNumber;
                    }
                }
            }
        }
    }
}