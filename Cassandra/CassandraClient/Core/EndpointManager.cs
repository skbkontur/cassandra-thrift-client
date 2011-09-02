using System;
using System.Linq;
using System.Net;

namespace CassandraClient.Core
{
    public class EndpointManager : IEndpointManager
    {
        public EndpointManager(IBadlist badlist)
        {
            this.badlist = badlist;
        }

        public void Register(IPEndPoint ipEndPoint)
        {
            badlist.Register(ipEndPoint);
        }

        public void Unregister(IPEndPoint ipEndPoint)
        {
            badlist.Unregister(ipEndPoint);
        }

        public void Good(IPEndPoint ipEndPoint)
        {
            badlist.Good(ipEndPoint);
        }

        public void Bad(IPEndPoint ipEndPoint)
        {
            badlist.Bad(ipEndPoint);
        }

        public IPEndPoint GetEndPoint()
        {
            var healthes = badlist.GetHealth();
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

        private readonly IBadlist badlist;

        [ThreadStatic]
        private static Random random;
        private static Random Random { get { return random ?? (random = new Random()); } }

        private const double eps = 1e-15;
    }
}