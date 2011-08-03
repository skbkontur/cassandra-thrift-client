using System;

namespace CassandraClient.StorageCore.Exceptions
{
    public class EmptySearchRequestException : StorageCoreException
    {
        public EmptySearchRequestException(string format, params object[] parameters)
            : base(format, parameters)
        {
        }

        public EmptySearchRequestException(Exception innerException, string format, params object[] parameters)
            : base(innerException, format, parameters)
        {
        }
    }
}