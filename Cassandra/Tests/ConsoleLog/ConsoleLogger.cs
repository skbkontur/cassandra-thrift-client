using System;

using SKBKontur.Cassandra.CassandraClient.Log;

namespace Cassandra.Tests.ConsoleLog
{
    public class ConsoleLogger : ICassandraLogger
    {
        public ConsoleLogger(string typeName)
        {
            this.typeName = typeName;
        }

        public void Debug(string message, params object[] args)
        {
            WriteMessage("DEBUG", null, message, args);
        }

        public void Debug(Exception exception, string message, params object[] args)
        {
            WriteMessage("DEBUG", exception, message, args);
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

        public void Debug(string message)
        {
            WriteMessage("DEBUG", null, message);
        }

        public void Debug(Exception exception, string message)
        {
            WriteMessage("DEBUG", exception, message);
        }

        public void Info(string message)
        {
            WriteMessage("INFO", null, message);
        }

        public void Info(Exception exception, string message)
        {
            WriteMessage("INFO", exception, message);
        }

        public void Warn(string message)
        {
            WriteMessage("WARN", null, message);
        }

        public void Warn(Exception exception, string message)
        {
            WriteMessage("WARN", exception, message);
        }

        public void Error(string message)
        {
            WriteMessage("ERROR", null, message);
        }

        public void Error(Exception exception, string message)
        {
            WriteMessage("ERROR", exception, message);
        }

        private void WriteMessage(string level, Exception exception, string message, params object[] args)
        {
            Console.WriteLine(string.Format("{0:HH:mm:ss.fff} {1} {2}: {3}", DateTime.Now, typeName, level, string.Format(message, args)));
            if (exception != null)
                Console.WriteLine(exception);
        }

        private void WriteMessage(string level, Exception exception, string message)
        {
            Console.Write(string.Format("{0:HH:mm:ss.fff} ", DateTime.Now));
            Console.WriteLine(" " + typeName + " " + level + ": " + message);
            if (exception != null)
                Console.WriteLine(exception);
        }

        private readonly string typeName;
    }
}