using System.Reflection;

using Thrift;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public class CassandraClientApplicationException : CassandraClientException
    {
        public CassandraClientApplicationException(string message)
            : base(message)
        {
        }

        public CassandraClientApplicationException(string message, TApplicationException innerException)
            : base(message + "\nType: " + GetFuckingType(innerException), innerException)
        {
        }

        private static string GetFuckingType(TApplicationException exception)
        {
            var field = typeof(TApplicationException).GetField("type", BindingFlags.Instance | BindingFlags.NonPublic);
            if(field != null)
            {
                var type = (TApplicationException.ExceptionType)(field.GetValue(exception));
                return type.ToString();
            }
            return "<failed to get type>";
        }
    }
}