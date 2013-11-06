using System.Net;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal interface IEndpointManager
    {
        void Register(IPEndPoint ipEndPoint);
        void Unregister(IPEndPoint ipEndPoint);
        void Good(IPEndPoint ipEndPoint);
        void Bad(IPEndPoint ipEndPoint);
        IPEndPoint[] GetEndPoints();
    }
}