﻿using System;

namespace CassandraClient.Exceptions
{
    public class CassandraClientTimedOutException : CassandraClientException
    {
        public CassandraClientTimedOutException(string message)
            : base(message)
        {
        }

        public CassandraClientTimedOutException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}