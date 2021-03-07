using System;

namespace Common
{
    public static class ConsoleHelper
    {
        public static void WriteInfo(string infoMessage)
        {
            var oldColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine(infoMessage);
            Console.ForegroundColor = oldColor;
        }

        public static void WriteWarning(string warningMessage)
        {
            var oldColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Yellow;
            Console.WriteLine(warningMessage);
            Console.ForegroundColor = oldColor;
        }

        public static void WriteError(string errorMessage)
        {
            var oldColor = Console.ForegroundColor;
            Console.ForegroundColor = ConsoleColor.Red;
            Console.WriteLine(errorMessage);
            Console.ForegroundColor = oldColor;
        }

        public static void WaitForEnterKey()
        {
            Console.WriteLine("Press enter to stop...");
            Console.ReadLine();
        }
    }
}