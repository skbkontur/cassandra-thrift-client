﻿namespace CassandraClient.StorageCore
{
    public interface IOneValueRepository
    {
        void Write(string value);
        string TryRead();
    }
}