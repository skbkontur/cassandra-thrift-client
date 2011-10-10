using System;

namespace SKBKontur.Cassandra.FunctionalTests.Logger
{
    public class ConsoleLogger
    {
        public ConsoleLogger(string typeName)
        {
            this.typeName = typeName;
        }

        public void Info(string message, params object[] args)
        {
            WriteMessage("INFO", null, message, args);
        }

        public void Info(Exception exception, string message, params object[] args)
        {
            WriteMessage("INFO", exception, message, args);
        }

        public void Warn(string message, params object[] args)
        {
            WriteMessage("WARN", null, message, args);
        }

        public void Warn(Exception exception, string message, params object[] args)
        {
            WriteMessage("WARN", exception, message, args);
        }

        public void Error(string message, params object[] args)
        {
            WriteMessage("ERROR", null, message, args);
        }

        public void Error(Exception exception, string message, params object[] args)
        {
            WriteMessage("ERROR", exception, message, args);
        }

        private void WriteMessage(string level, Exception exception, string message, params object[] args)
        {
            Console.WriteLine(string.Format("{0:HH:mm:ss.fff} {1} {2}: {3}", DateTime.Now, typeName, level, string.Format(message, args)));
            if(exception != null)
                Console.WriteLine(exception);
        }

        private readonly string typeName;
    }
}