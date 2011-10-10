using System;

namespace SKBKontur.Cassandra.CassandraClient.Log
{
    public interface ICassandraLogger
    {
        void Debug(string message, params object[] args);
        void Debug(Exception exception, string message, params object[] args);
        void Info(string message, params object[] args);
        void Info(Exception exception, string message, params object[] args);
        void Warn(string message, params object[] args);
        void Warn(Exception exception, string message, params object[] args);
        void Error(string message, params object[] args);
        void Error(Exception exception, string message, params object[] args);

        void Debug(string message);
        void Debug(Exception exception, string message);
        void Info(string message);
        void Info(Exception exception, string message);
        void Warn(string message);
        void Warn(Exception exception, string message);
        void Error(string message);
        void Error(Exception exception, string message);
    }
}