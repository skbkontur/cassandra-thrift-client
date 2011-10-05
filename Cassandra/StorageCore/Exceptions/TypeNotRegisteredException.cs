using System;

namespace StorageCore.Exceptions
{
    public class TypeNotRegisteredException : StorageCoreException
    {
        public TypeNotRegisteredException(string format, params object[] parameters)
            : base(format, parameters)
        {
        }

        public TypeNotRegisteredException(Exception innerException, string format, params object[] parameters)
            : base(innerException, format, parameters)
        {
        }
    }
}