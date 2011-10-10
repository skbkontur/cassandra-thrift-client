using System;
using System.Linq;
using System.Net;

using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public class EndpointManager : IEndpointManager
    {
        public EndpointManager(IBadlist badlist, ICassandraLogManager logManager)
        {
            this.badlist = badlist;
            logger = logManager.GetLogger(GetType());
        }

        public void Register(IPEndPoint ipEndPoint)
        {
            badlist.Register(ipEndPoint);
            logger.Info("New node with endpoint {0} was added in client topology.", ipEndPoint);
        }

        public void Unregister(IPEndPoint ipEndPoint)
        {
            badlist.Unregister(ipEndPoint);
            logger.Info("Node with endpoint {0} was deleted from client topology.", ipEndPoint);
        }

        public void Good(IPEndPoint ipEndPoint)
        {
            badlist.Good(ipEndPoint);
            logger.Debug("Health of node with endpoint {0} was increased. Current health: {1}", ipEndPoint, badlist.GetHealth(ipEndPoint));
        }

        public void Bad(IPEndPoint ipEndPoint)
        {
            badlist.Bad(ipEndPoint);
            logger.Debug("Health of node with endpoint {0} was decreased. Current health: {1}", ipEndPoint, badlist.GetHealth(ipEndPoint));
        }

        public IPEndPoint GetEndPoint()
        {
            var healthes = badlist.GetHealthes();
            var sum = healthes.Sum(h => h.Value);

            double rnd = Random.NextDouble();
            foreach(var t in healthes)
            {
                rnd -= t.Value / sum;
                if(rnd < eps)
                    return t.Key;
            }
            return healthes.Last().Key;
        }

        private static Random Random { get { return random ?? (random = new Random()); } }

        private readonly IBadlist badlist;

        [ThreadStatic]
        private static Random random;

        private readonly ICassandraLogger logger;

        private const double eps = 1e-15;
    }
}