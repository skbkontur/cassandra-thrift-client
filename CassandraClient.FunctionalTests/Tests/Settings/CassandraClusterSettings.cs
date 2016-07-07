﻿using System;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Clusters;
using SKBKontur.Cassandra.FunctionalTests.Tests;

namespace SKBKontur.Cassandra.FunctionalTests.Settings
{
    public class CassandraClusterSettings : ICassandraClusterSettings
    {
        public IPEndPoint[] Endpoints { get { return new[]
            {
                new IPEndPoint(new IPAddress(new byte[]{127, 0, 0, 1}), 9898)
                /*new IPEndPoint(new IPAddress(new byte[] {192, 168, 89, 157}), 9898),
                new IPEndPoint(new IPAddress(new byte[] {192, 168, 88, 155}), 9898),
                new IPEndPoint(new IPAddress(new byte[] {192, 168, 89, 212}), 9898),
                new IPEndPoint(new IPAddress(new byte[] {192, 168, 90, 21}), 9898),
                new IPEndPoint(new IPAddress(new byte[] {192, 168, 90, 33}), 9898)*/
            }; } }

        public IPEndPoint EndpointForFierceCommands { get { return Endpoints[0]; } }
        public bool AllowNullTimestamp { get { return true; } }
        public int Attempts { get { return 5; } }
        public int Timeout { get { return 6000; } }
        public int FierceTimeout { get { return 10000; } }
        public TimeSpan? ConnectionIdleTimeout { get { return TimeSpan.FromSeconds(30); } }
        public string ClusterName { get { return Constants.ClusterName; } }
        public ConsistencyLevel ReadConsistencyLevel { get { return ConsistencyLevel.ALL; } }
        public ConsistencyLevel WriteConsistencyLevel { get { return ConsistencyLevel.ALL; } }
    }
}