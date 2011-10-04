using System;

namespace CassandraClient.AquilesTrash.Exceptions
{
    /// <summary>
    /// Exception thrown when something went wrong inside Aquiles
    /// </summary>
    [Serializable]
    public class AquilesException : System.Exception
    {
        /// <summary>
        /// ctor
        /// </summary>
        public AquilesException() : base() { }
        /// <summary>
        /// ctor
        /// </summary>
        public AquilesException(string message) : base(message) { }
        /// <summary>
        /// ctor
        /// </summary>
        public AquilesException(string message, System.Exception ex) : base(message, ex) { }
    }
}
