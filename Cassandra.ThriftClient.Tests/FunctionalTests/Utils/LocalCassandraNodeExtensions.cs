using System;
using System.Net;

using SkbKontur.Cassandra.Local;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Utils
{
    public static class LocalCassandraNodeExtensions
    {
        public static SingleNodeCassandraClusterSettings CreateSettings(this LocalCassandraNode node)
        {
            var thriftEndpoint = new IPEndPoint(IPAddress.Parse(node.RpcAddress), node.RpcPort);
            return new SingleNodeCassandraClusterSettings
                {
                    ClusterName = node.ClusterName,
                    ReadConsistencyLevel = ConsistencyLevel.QUORUM,
                    WriteConsistencyLevel = ConsistencyLevel.QUORUM,
                    Endpoints = new[] {thriftEndpoint},
                    EndpointForFierceCommands = thriftEndpoint,
                    AllowNullTimestamp = false,
                    Attempts = 5,
                    Timeout = (int)TimeSpan.FromSeconds(6).TotalMilliseconds,
                    FierceTimeout = (int)TimeSpan.FromSeconds(10).TotalMilliseconds,
                    ConnectionIdleTimeout = TimeSpan.FromSeconds(30),
                    EnableMetrics = false
                };
        }

        public class SingleNodeCassandraClusterSettings : ICassandraClusterSettings
        {
            public string ClusterName { get; set; }
            public ConsistencyLevel ReadConsistencyLevel { get; set; }
            public ConsistencyLevel WriteConsistencyLevel { get; set; }
            public IPEndPoint[] Endpoints { get; set; }
            public IPEndPoint EndpointForFierceCommands { get; set; }
            public bool AllowNullTimestamp { get; set; }
            public int Attempts { get; set; }
            public int Timeout { get; set; }
            public int FierceTimeout { get; set; }
            public TimeSpan? ConnectionIdleTimeout { get; set; }
            public bool EnableMetrics { get; set; }
        }
    }
}