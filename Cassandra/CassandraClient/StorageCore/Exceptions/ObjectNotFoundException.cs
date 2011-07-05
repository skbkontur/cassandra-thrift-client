using System;

namespace CassandraClient.StorageCore.Exceptions
{
    public class ObjectNotFoundException : StorageCoreException
    {
        public ObjectNotFoundException(string format, params object[] parameters)
            : base(format, parameters)
        {
        }

        public ObjectNotFoundException(Exception innerException, string format, params object[] parameters)
            : base(innerException, format, parameters)
        {
        }
    }
}