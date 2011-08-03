using System.Collections.Generic;
using System.Net;

namespace CassandraClient.Core
{
    public interface IBadlist
    {
        void Good(IPEndPoint endPoint);
        void Bad(IPEndPoint endPoint);
        KeyValuePair<IPEndPoint, double>[] GetHealth();
        void Register(IPEndPoint endPoint);
        void Unregister(IPEndPoint endPoint);
    }
}