﻿using System;

namespace CassandraClient.Exceptions
{
    public class CassandraClientUnavailableException : CassandraClientException
    {
        public CassandraClientUnavailableException(string message)
            : base(message)
        {
        }

        public CassandraClientUnavailableException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}