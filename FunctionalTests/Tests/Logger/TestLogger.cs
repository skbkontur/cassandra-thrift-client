using System;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Log;

using log4net;

namespace SKBKontur.Cassandra.FunctionalTests.Logger
{
    public class TestLogger : ICassandraLogger
    {
        public TestLogger(string name)
        {
            logger = LogManager.GetLogger(name);
        }

        public void Debug(string message, params object[] args)
        {
            logger.Debug(GetRealMessage(message, args));
        }

        public void Debug(Exception exception, string message, params object[] args)
        {
            logger.Debug(GetRealMessage(message, args), exception);
        }

        public void Info(string message, params object[] args)
        {
            logger.Info(GetRealMessage(message, args));
        }

        public void Info(Exception exception, string message, params object[] args)
        {
            logger.Info(GetRealMessage(message, args), exception);
        }

        public void Warn(string message, params object[] args)
        {
            logger.Warn(GetRealMessage(message, args));
        }

        public void Warn(Exception exception, string message, params object[] args)
        {
            logger.Warn(GetRealMessage(message, args), exception);
        }

        public void Error(string message, params object[] args)
        {
            logger.Error(GetRealMessage(message, args));
        }

        public void Error(Exception exception, string message, params object[] args)
        {
            logger.Error(GetRealMessage(message, args), exception);
        }

        public void Debug(string message)
        {
            logger.Debug(GetRealMessage(message));
        }

        public void Debug(Exception exception, string message)
        {
            logger.Debug(GetRealMessage(message), exception);
        }

        public void Info(string message)
        {
            logger.Info(GetRealMessage(message));
        }

        public void Info(Exception exception, string message)
        {
            logger.Info(GetRealMessage(message), exception);
        }

        public void Warn(string message)
        {
            logger.Warn(GetRealMessage(message));
        }

        public void Warn(Exception exception, string message)
        {
            logger.Warn(GetRealMessage(message), exception);
        }

        public void Error(string message)
        {
            logger.Error(GetRealMessage(message));
        }

        public void Error(Exception exception, string message)
        {
            logger.Error(GetRealMessage(message), exception);
        }

        private string GetRealMessage(string message, params object[] args)
        {
            return TestNameHolder.TestName + " " + string.Format(message, args);
        }

        private string GetRealMessage(string message)
        {
            return TestNameHolder.TestName + " " + message;
        }

        private readonly ILog logger;
    }
}