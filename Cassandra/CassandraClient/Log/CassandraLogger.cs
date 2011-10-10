using System;

using log4net;

namespace SKBKontur.Cassandra.CassandraClient.Log
{
    public class CassandraLogger : ICassandraLogger
{
        public CassandraLogger(ILog logger)
        {
            this.logger = logger;
        }
        
        public void Debug(string message, params object[] args)
        {
            logger.Debug(string.Format(message, args));
        }

        public void Debug(Exception exception, string message, params object[] args)
        {
            logger.Debug(string.Format(message, args), exception);
        }

        public void Info(string message, params object[] args)
        {
            logger.Info(string.Format(message, args));
        }

        public void Info(Exception exception, string message, params object[] args)
        {
            logger.Info(string.Format(message, args), exception);
        }

        public void Warn(string message, params object[] args)
        {
            logger.Warn(string.Format(message, args));
        }

        public void Warn(Exception exception, string message, params object[] args)
        {
            logger.Warn(string.Format(message, args), exception);
        }

        public void Error(string message, params object[] args)
        {
            logger.Error(string.Format(message, args));
        }

        public void Error(Exception exception, string message, params object[] args)
        {
            logger.Error(string.Format(message, args), exception);
        }

        public void Debug(string message)
        {
            logger.Debug(message);
        }

        public void Debug(Exception exception, string message)
        {
            logger.Debug(message, exception);
        }

        public void Info(string message)
        {
            logger.Info(message);
        }

        public void Info(Exception exception, string message)
        {
            logger.Info(message, exception);
        }

        public void Warn(string message)
        {
            logger.Warn(message);
        }

        public void Warn(Exception exception, string message)
        {
            logger.Warn(message, exception);
        }

        public void Error(string message)
        {
            logger.Error(message);
        }

        public void Error(Exception exception, string message)
        {
            logger.Error(message, exception);
        }

        private readonly ILog logger;
    }
}