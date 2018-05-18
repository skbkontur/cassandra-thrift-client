using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Clusters
{
    public interface ICassandraClusterSettings
    {
        string ClusterName { get; }
        ConsistencyLevel ReadConsistencyLevel { get; }
        ConsistencyLevel WriteConsistencyLevel { get; }
        IPEndPoint[] Endpoints { get; }
        IPEndPoint EndpointForFierceCommands { get; }
        bool AllowNullTimestamp { get; }
        int Attempts { get; }
        int Timeout { get; }
        int FierceTimeout { get; }
        TimeSpan? ConnectionIdleTimeout { get; }
        bool EnableMetrics { get; }
    }
}