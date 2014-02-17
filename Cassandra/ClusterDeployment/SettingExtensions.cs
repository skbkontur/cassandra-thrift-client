using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;

namespace SKBKontur.Cassandra.ClusterDeployment
{
    public static class SettingExtensions
    {
        public static ICassandraClusterSettings CreateSettings(this CassandraNode node, IPAddress nodeAddress)
        {
            return new CassandraSingleNodeClusterSettings
                {
                    AllowNullTimestamp = false,
                    ClusterName = node.ClusterName,
                    Attempts = 5,
                    ConnectionIdleTimeout = TimeSpan.FromMinutes(1),
                    EndpointForFierceCommands = new IPEndPoint(nodeAddress, node.RpcPort),
                    Endpoints = new[] {new IPEndPoint(nodeAddress, node.RpcPort)},
                    FierceTimeout = (int)TimeSpan.FromSeconds(10).TotalMilliseconds,
                    ReadConsistencyLevel = ConsistencyLevel.QUORUM,
                    WriteConsistencyLevel = ConsistencyLevel.QUORUM,
                    Timeout = (int)TimeSpan.FromSeconds(6).TotalMilliseconds
                };
        }

        private class CassandraSingleNodeClusterSettings : ICassandraClusterSettings
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
        }
    }
}