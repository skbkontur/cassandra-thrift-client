using System;
using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    internal static class ItemHealthExtensions
    {
        public static T2 RandomItemByHealth<T, T2>(this IEnumerable<T> items, Func<T, double> healthSelector, Func<T, T2> resultSelector)
        {
            return items.ShuffleByHealth(healthSelector, resultSelector).First();
        }

        public static IEnumerable<T2> ShuffleByHealth<T, T2>(this IEnumerable<T> items, Func<T, double> healthSelector, Func<T, T2> resultSelector)
        {
            var itemsWithHealth = new HashSet<KeyValuePair<T, double>>(items.Select(x => new KeyValuePair<T, double>(x, healthSelector(x))));
            var totalItemListLength = itemsWithHealth.Count;

            for (var i = 0; i < totalItemListLength; i++)
            {
                var result = default(T2);
                var healthSum = itemsWithHealth.Sum(h => h.Value);

                var valueFound = false;
                var randomValue = Random.NextDouble();
                foreach (var itemWithHealth in itemsWithHealth)
                {
                    randomValue -= itemWithHealth.Value / healthSum;
                    if(randomValue < epsilon)
                    {
                        valueFound = true;
                        result = resultSelector(itemWithHealth.Key);
                        itemsWithHealth.Remove(itemWithHealth);
                        break;
                    }
                }
                if (!valueFound)
                {
                    var last = itemsWithHealth.Last();
                    result = resultSelector(last.Key);
                    itemsWithHealth.Remove(last);
                }
                yield return result;
            }
        }

        public static IEnumerable<T> ShuffleByHealth<T>(this IEnumerable<T> items, Func<T, double> healthSelector)
        {
            return items.ShuffleByHealth(healthSelector, x => x);
        }

        private static Random Random { get { return random ?? (random = new Random()); } }

        [ThreadStatic]
        private static Random random;

        private const double epsilon = 1e-15;
    }
}