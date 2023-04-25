using System;
using System.Linq;
using System.Net;

using JetBrains.Annotations;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Clusters;

namespace SkbKontur.Cassandra.ThriftClient.Tests.FunctionalTests.Utils
{
    public static class LocalCassandraNodeExtensions
    {
        public static SingleNodeCassandraClusterSettings CreateSettings()
        {
            var thriftEndpoint = new IPEndPoint(GetIpV4Address("127.0.0.1"), 9160);
            return new SingleNodeCassandraClusterSettings
                {
                    ClusterName = "TestCluster",
                    ReadConsistencyLevel = ConsistencyLevel.QUORUM,
                    WriteConsistencyLevel = ConsistencyLevel.QUORUM,
                    Endpoints = new[] {thriftEndpoint},
                    EndpointForFierceCommands = thriftEndpoint,
                    AllowNullTimestamp = false,
                    Attempts = 5,
                    Timeout = (int)TimeSpan.FromSeconds(6).TotalMilliseconds,
                    FierceTimeout = (int)TimeSpan.FromSeconds(10).TotalMilliseconds,
                    ConnectionIdleTimeout = TimeSpan.FromSeconds(30),
                    EnableMetrics = false,
                    Credentials = new Credentials("cassandra", "cassandra"),
                };
        }

        private static IPAddress GetIpV4Address([NotNull] string hostNameOrIpAddress)
        {
            if (IPAddress.TryParse(hostNameOrIpAddress, out var res))
                return res;

            return Dns.GetHostEntry(hostNameOrIpAddress).AddressList.First(address => !address.ToString().Contains(':'));
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

            [CanBeNull]
            public Credentials Credentials { get; set; }
        }
    }
}