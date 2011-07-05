﻿using System;

namespace CassandraClient.Exceptions
{
    public class CassandraClientAuthenticationException : CassandraClientException
    {
        public CassandraClientAuthenticationException(string message)
            : base(message)
        {
        }

        public CassandraClientAuthenticationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}