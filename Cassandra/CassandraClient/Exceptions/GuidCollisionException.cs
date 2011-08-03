﻿using System;

namespace CassandraClient.Exceptions
{
    public class GuidCollisionException : Exception
    {
        public GuidCollisionException(Guid id)
            : base(string.Format("Id = {0}", id))
        {
        }
    }
}