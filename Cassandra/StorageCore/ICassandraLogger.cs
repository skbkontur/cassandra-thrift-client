using System;

namespace SKBKontur.Cassandra.StorageCore
{
    public interface ICassandraLogger
    {
        void Info(string message, params object[] args);
        void Info(Exception exception, string message, params object[] args);
        void Warn(string message, params object[] args);
        void Warn(Exception exception, string message, params object[] args);
        void Error(string message, params object[] args);
        void Error(Exception exception, string message, params object[] args);
    }
}