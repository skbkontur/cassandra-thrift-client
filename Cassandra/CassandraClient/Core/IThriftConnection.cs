using System;

using CassandraClient.Abstractions;

namespace CassandraClient.Core
{
    public interface IThriftConnection : IDisposable
    {
        void ExecuteCommand(ICommand command);
    }
}