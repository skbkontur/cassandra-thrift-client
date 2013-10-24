using System;
using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    public static class ItemHealthExtensions
    {
        public static IEnumerable<T2> ShuffleByHealth<T, T2>(this IEnumerable<T> items, Func<T, double> healthSelector, Func<T, T2> resultSelector) where T2 : class
        {
            var itemsWithHealth = new HashSet<KeyValuePair<T, double>>(items.Select(x => new KeyValuePair<T, double>(x, healthSelector(x))));

            for (var i = 0; i < itemsWithHealth.Count; i++)
            {
                T2 result = null;
                var healthSum = itemsWithHealth.Sum(h => h.Value);

                var randomValue = Random.NextDouble();
                foreach (var itemWithHealth in itemsWithHealth)
                {
                    randomValue -= itemWithHealth.Value / healthSum;
                    if(randomValue < epsilon)
                    {
                        result = resultSelector(itemWithHealth.Key);
                        itemsWithHealth.Remove(itemWithHealth);
                        break;
                    }
                }
                if (result == null)
                {
                    var last = itemsWithHealth.Last();
                    result = resultSelector(last.Key);
                    itemsWithHealth.Remove(last);
                }
                yield return result;
            }
        }

        public static IEnumerable<T> ShuffleByHealth<T>(this IEnumerable<T> items, Func<T, double> healthSelector) where T: class
        {
            return items.ShuffleByHealth(healthSelector, x => x);
        }

        private static Random Random { get { return random ?? (random = new Random()); } }

        [ThreadStatic]
        private static Random random;

        private const double epsilon = 1e-15;
    }
}