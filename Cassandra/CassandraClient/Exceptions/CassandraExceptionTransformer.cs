using System;
using System.IO;

using Apache.Cassandra;

using Thrift;
using Thrift.Transport;

namespace SKBKontur.Cassandra.CassandraClient.Exceptions
{
    public static class CassandraExceptionTransformer
    {
        public static Exception Transform(Exception e, string message)
        {
            if (e is NotFoundException)
                return new CassandraClientSomethingNotFoundException(message, e);
            if (e is InvalidRequestException)
                return new CassandraClientInvalidRequestException(message, (InvalidRequestException)e);
            if (e is UnavailableException)
                return new CassandraClientUnavailableException(message, e);
            if (e is TimedOutException)
                return new CassandraClientTimedOutException(message, e);
            if (e is TApplicationException)
                return new CassandraClientApplicationException(message, (TApplicationException)e);
            if (e is AuthenticationException)
                return new CassandraClientAuthenticationException(message, e);
            if (e is AuthorizationException)
                return new CassandraClientAuthorizationException(message, e);
            if (e is TTransportException)
                return new CassandraClientTransportException(message, e);
            if (e is IOException)
                return new CassandraClientIOException(message, e);
            return new CassandraUnknownException(message, e);
        }
    }
}