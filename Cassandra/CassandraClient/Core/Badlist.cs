using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;

namespace CassandraClient.Core
{
    public class Badlist : IBadlist
    {
        public void Good(IPEndPoint endPoint)
        {
            Health health;
            if (healths.TryGetValue(endPoint, out health))
            {
                var healthValue = health.Value * alivingRate;
                if (healthValue > aliveHealth) healthValue = aliveHealth;
                health.Value = healthValue;
            }
        }

        public void Bad(IPEndPoint endPoint)
        {
            Health health;
            if (healths.TryGetValue(endPoint, out health))
            {
                var healthValue = health.Value * dyingRate;
                if (healthValue < deadHealth) healthValue = deadHealth;
                health.Value = healthValue;
            }
        }

        public KeyValuePair<IPEndPoint, double>[] GetHealthes()
        {
            var result = new List<KeyValuePair<IPEndPoint, double>>();
            foreach (IPEndPoint key in healths.Keys)
            {
                Health healht;
                if (healths.TryGetValue(key, out healht))
                    result.Add(new KeyValuePair<IPEndPoint, double>(key, healht.Value));
            }
            return result.ToArray();
        }

        public double GetHealth(IPEndPoint endPoint)
        {
            Health health;
            if (healths.TryGetValue(endPoint, out health))
                return health.Value;
            return 0;
        }

        public void Register(IPEndPoint endPoint)
        {
            SetHealth(endPoint, aliveHealth);
        }

        public void Unregister(IPEndPoint endPoint)
        {
            Health health;
            healths.TryRemove(endPoint, out health);
        }

        private void SetHealth(IPEndPoint endPoint, double health)
        {
            var healthObj = healths.GetOrAdd(endPoint, ipEndpoint => new Health());
            healthObj.Value = health;
        }

        private readonly ConcurrentDictionary<IPEndPoint, Health> healths = new ConcurrentDictionary<IPEndPoint, Health>(); 
        private const double aliveHealth = 1.0;
        private const double deadHealth = 0.01;
        private const double alivingRate = 1.5;
        private const double dyingRate = 0.7;
    }
}