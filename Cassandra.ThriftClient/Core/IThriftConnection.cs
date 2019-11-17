using System;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Core.GenericPool;

namespace SkbKontur.Cassandra.ThriftClient.Core
{
    internal interface IThriftConnection : IDisposable, ILiveness
    {
        void ExecuteCommand(ICommand command);
    }
}