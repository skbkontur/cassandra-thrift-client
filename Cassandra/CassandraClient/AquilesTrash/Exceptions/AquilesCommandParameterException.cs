using System;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions
{
    [Serializable]
    public class AquilesCommandParameterException : AquilesException
    {
        public AquilesCommandParameterException(string message) : base(message) { }
        public AquilesCommandParameterException(string format, params object[] args) : base(format, args) { }
    }
}
