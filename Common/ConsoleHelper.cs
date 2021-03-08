using System;

namespace Common
{
    public static class ConsoleHelper
    {
        private static readonly object _lock = new();

        public static void WriteInfo(string infoMessage)
        {
            lock (_lock)
            {
                var oldColor = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Green;
                Console.WriteLine(infoMessage);
                Console.ForegroundColor = oldColor;
            }
        }

        public static void WriteWarning(string warningMessage)
        {
            lock (_lock)
            {
                var oldColor = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Yellow;
                Console.WriteLine(warningMessage);
                Console.ForegroundColor = oldColor;
            }
        }

        public static void WriteError(string errorMessage)
        {
            lock (_lock)
            {
                var oldColor = Console.ForegroundColor;
                Console.ForegroundColor = ConsoleColor.Red;
                Console.WriteLine(errorMessage);
                Console.ForegroundColor = oldColor;
            }
        }

        public static void WaitForEnterKey()
        {
            Console.WriteLine("Press enter to stop...");
            Console.ReadLine();
        }
    }
}