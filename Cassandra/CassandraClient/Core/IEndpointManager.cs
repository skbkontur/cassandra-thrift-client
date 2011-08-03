using System.Net;

namespace CassandraClient.Core
{
    public interface IEndpointManager
    {
        void Register(IPEndPoint ipEndPoint);
        void Unregister(IPEndPoint ipEndPoint);
        void Good(IPEndPoint ipEndPoint);
        void Bad(IPEndPoint ipEndPoint);
        IPEndPoint GetEndPoint();
    }
}