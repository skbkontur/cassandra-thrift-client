using System;

namespace CassandraClient.AquilesTrash.Exceptions
{
    /// <summary>
    /// Exception thrown when a command input parameters are not valid
    /// </summary>
    [Serializable]
    public class AquilesCommandException : AquilesException
    {
        public AquilesCommandException(){ }
        public AquilesCommandException(string message) : base(message) { }
        public AquilesCommandException(string format, params object[] args) : base(format, args) { }
        public AquilesCommandException(string message, Exception ex) : base(message, ex) { }
    }
}
