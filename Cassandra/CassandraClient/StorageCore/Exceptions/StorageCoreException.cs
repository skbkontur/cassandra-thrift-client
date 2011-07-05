using System;

namespace CassandraClient.StorageCore.Exceptions
{
    public class StorageCoreException : Exception
    {
        public StorageCoreException(string format, params object[] parameters)
            : base(String.Format(format, parameters))
        {
        }

        public StorageCoreException(Exception innerException, string format, params object[] parameters)
            : base(String.Format(format, parameters), innerException)
        {
        }
    }
}