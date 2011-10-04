using System;

namespace CassandraClient.AquilesTrash.Exceptions
{
    /// <summary>
    /// Exception thrown when a command input parameters are not valid
    /// </summary>
    [Serializable]
    public class AquilesCommandException : AquilesException
    {
        /// <summary>
        /// ctor
        /// </summary>
        public AquilesCommandException() : base() { }
        /// <summary>
        /// ctor
        /// </summary>
        public AquilesCommandException(string message) : base(message) { }
        /// <summary>
        /// ctor
        /// </summary>
        public AquilesCommandException(string message, System.Exception ex) : base(message, ex) { }
    }
}
