using System.Net;

using CassandraClient.Abstractions;
using CassandraClient.Clusters;

using Tests.Tests;

namespace Tests.Settings
{
    class CassandraClusterSettings : ICassandraClusterSettings
    {
        public string ClusterName { get { return Constants.ClusterName; } }

        public ConsistencyLevel ReadConsistencyLevel { get { return ConsistencyLevel.QUORUM; } }

        public ConsistencyLevel WriteConsistencyLevel { get { return ConsistencyLevel.QUORUM; } }

        public IPEndPoint[] Endpoints { get { return new[] {new IPEndPoint(new IPAddress(new byte[] {127, 0, 0, 1}), 9898)}; } }

        public int Attempts { get { return 5; } }

        public int Timeout { get { return 6000; } }
    }
}