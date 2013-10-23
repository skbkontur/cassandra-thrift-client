using System;
using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    public static class ItemHealthExtensions
    {
        public static IEnumerable<T> ShuffleByHealth<T>(this IEnumerable<T> items, Func<T, double> healthSelector) where T : class
        {
            var itemsWithHealth = new HashSet<KeyValuePair<T, double>>(items.Select(x => new KeyValuePair<T, double>(x, healthSelector(x))));
            var result = new T[itemsWithHealth.Count];

            for(var i = 0; i < result.Length; i++)
            {
                var healthSum = itemsWithHealth.Sum(h => h.Value);

                var randomValue = Random.NextDouble();
                foreach (var itemWithHealth in itemsWithHealth)
                {
                    randomValue -= itemWithHealth.Value / healthSum;
                    if(randomValue < epsilon)
                    {
                        result[i] = itemWithHealth.Key;
                        itemsWithHealth.Remove(itemWithHealth);
                        break;
                    }
                }
                if(result[i] == null)
                {
                    var last = itemsWithHealth.Last();
                    result[i] = last.Key;
                    itemsWithHealth.Remove(last);
                }
            }
            return result;
        }

        private static Random Random { get { return random ?? (random = new Random()); } }

        [ThreadStatic]
        private static Random random;

        private const double epsilon = 1e-15;
    }
}