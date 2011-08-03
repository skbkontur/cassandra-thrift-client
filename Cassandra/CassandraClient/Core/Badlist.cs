using System.Collections;
using System.Collections.Generic;
using System.Net;

namespace CassandraClient.Core
{
    public class Badlist : IBadlist
    {
        public void Good(IPEndPoint endPoint)
        {
            object value = healths[endPoint];
            if (value == null || (double)value >= aliveHealth)
                return;
            lock (healthsLock)
            {
                double health = (double)value * alivingRate;
                if (health > aliveHealth) health = aliveHealth;
                healths[endPoint] = health;
            }
        }

        public void Bad(IPEndPoint endPoint)
        {
            object value = healths[endPoint];
            double health = value == null ? aliveHealth : (double)value;
            lock (healthsLock)
            {
                health = health * dyingRate;
                if (health < deadHealth) health = deadHealth;
                healths[endPoint] = health;
            }
        }

        public KeyValuePair<IPEndPoint, double>[] GetHealth()
        {
            var result = new List<KeyValuePair<IPEndPoint, double>>();
            foreach (IPEndPoint key in healths.Keys)
            {
                var value = healths[key];
                if (value != null)
                    result.Add(new KeyValuePair<IPEndPoint, double>(key, (double)value));
            }
            return result.ToArray();
        }

        public void Register(IPEndPoint endPoint)
        {
            SetHealth(endPoint, aliveHealth);
        }

        public void Unregister(IPEndPoint endPoint)
        {
            lock (healthsLock)
            {
                healths.Remove(endPoint);
            }
        }

        private void SetHealth(IPEndPoint endPoint, double health)
        {
            lock (healthsLock)
            {
                healths[endPoint] = health;
            }
        }

        private readonly Hashtable healths = new Hashtable();
        private readonly object healthsLock = new object();
        private const double aliveHealth = 1.0;
        private const double deadHealth = 0.1;
        private const double alivingRate = 1.5;
        private const double dyingRate = 0.7;
    }
}