﻿using System;

namespace CassandraClient.Exceptions
{
    public class CassandraClientAuthorizationException : CassandraClientException
    {
        public CassandraClientAuthorizationException(string message)
            : base(message)
        {
        }

        public CassandraClientAuthorizationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}