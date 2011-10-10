using System;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions
{
    /// <summary>
    /// Exception thrown when something went wrong inside Aquiles
    /// </summary>
    [Serializable]
    public class AquilesException : Exception
    {
        protected AquilesException(){ }
        public AquilesException(string message) : base(message) { }
        public AquilesException(string format, params object[] args) : base(string.Format(format, args)) { }
        protected AquilesException(string message, Exception ex) : base(message, ex) { }
    }
}
