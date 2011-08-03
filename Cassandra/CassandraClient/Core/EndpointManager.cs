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
            random = new Random();
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
            badlist.Good(ipEndPoint);
        }

        public IPEndPoint GetEndPoint()
        {
            var health = badlist.GetHealth();
            var sum = health.Sum(h => h.Value);

            double rnd;
            lock(lockObject)
            {
                rnd = random.NextDouble();
            }
            foreach(var t in health)
            {
                rnd -= t.Value / sum;
                if(Math.Abs(rnd) < eps)
                    return t.Key;
            }
            return health.Last().Key;
        }

        private readonly IBadlist badlist;
        private readonly Random random;
        private readonly object lockObject = new object();
        private const double eps = 1e-9;
    }
}