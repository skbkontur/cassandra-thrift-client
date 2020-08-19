using System;
using System.Net;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Clusters
{
    [PublicAPI]
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

        [CanBeNull]
        Credentials Credentials { get; }
    }
}