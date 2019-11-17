using System;

namespace SkbKontur.Cassandra.ThriftClient.Core.GenericPool.Exceptions
{
    internal class FailedReleaseItemException : Exception
    {
        public FailedReleaseItemException(string message)
            : base(message)
        {
        }
    }
}