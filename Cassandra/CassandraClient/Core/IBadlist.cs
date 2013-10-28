using System.Collections.Generic;
using System.Net;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal interface IBadlist
    {
        void Good(IPEndPoint endPoint);
        void Bad(IPEndPoint endPoint);
        KeyValuePair<IPEndPoint, double>[] GetHealthes();
        double GetHealth(IPEndPoint endPoint);
        void Register(IPEndPoint endPoint);
        void Unregister(IPEndPoint endPoint);
    }
}