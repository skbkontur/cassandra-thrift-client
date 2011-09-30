using System.Net;

using CassandraClient.Abstractions;

namespace CassandraClient.Clusters
{
    public interface ICassandraClusterSettings
    {
        string ClusterName { get; }
        ConsistencyLevel ReadConsistencyLevel { get; }
        ConsistencyLevel WriteConsistencyLevel { get; }
        IPEndPoint[] Endpoints { get;  }
        int Attempts { get; }
        int Timeout { get; }
    }
}