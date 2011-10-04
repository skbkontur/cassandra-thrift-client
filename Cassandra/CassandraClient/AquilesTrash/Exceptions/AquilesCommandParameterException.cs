using System;

namespace CassandraClient.AquilesTrash.Exceptions
{
    /// <summary>
    /// Exception thrown when a command input parameters are not valid
    /// </summary>
    [Serializable]
    public class AquilesCommandParameterException : AquilesException
    {
       /// <summary>
        /// ctor
        /// </summary>
        public AquilesCommandParameterException() : base() { }
        /// <summary>
        /// ctor
        /// </summary>
        public AquilesCommandParameterException(string message) : base(message) { }
        /// <summary>
        /// ctor
        /// </summary>
        public AquilesCommandParameterException(string message, System.Exception ex) : base(message, ex) { }
    }
}
