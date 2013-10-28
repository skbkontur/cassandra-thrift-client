using System.Linq;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Helpers;

using log4net;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal class EndpointManager : IEndpointManager
    {
        public EndpointManager(IBadlist badlist)
        {
            this.badlist = badlist;
        }

        public void Register(IPEndPoint ipEndPoint)
        {
            badlist.Register(ipEndPoint);
            logger.InfoFormat("New node with endpoint {0} was added in client topology.", ipEndPoint);
        }

        public void Unregister(IPEndPoint ipEndPoint)
        {
            badlist.Unregister(ipEndPoint);
            logger.InfoFormat("Node with endpoint {0} was deleted from client topology.", ipEndPoint);
        }

        public void Good(IPEndPoint ipEndPoint)
        {
            badlist.Good(ipEndPoint);
            logger.DebugFormat("Health of node with endpoint {0} was increased. Current health: {1}", ipEndPoint, badlist.GetHealth(ipEndPoint));
        }

        public void Bad(IPEndPoint ipEndPoint)
        {
            badlist.Bad(ipEndPoint);
            logger.DebugFormat("Health of node with endpoint {0} was decreased. Current health: {1}", ipEndPoint, badlist.GetHealth(ipEndPoint));
        }

        public IPEndPoint[] GetEndPoints()
        {
            return badlist.GetHealthes().ShuffleByHealth(x => x.Value, x => x.Key).ToArray();
        }

        private readonly IBadlist badlist;
        private readonly ILog logger = LogManager.GetLogger(typeof(EndpointManager));
    }
}