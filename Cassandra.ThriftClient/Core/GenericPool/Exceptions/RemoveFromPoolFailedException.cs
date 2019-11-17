using System;

namespace SkbKontur.Cassandra.ThriftClient.Core.GenericPool.Exceptions
{
    public class RemoveFromPoolFailedException : Exception
    {
        public RemoveFromPoolFailedException(string message)
            : base(message)
        {
        }
    }
}