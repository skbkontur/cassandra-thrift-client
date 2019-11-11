using System;
using System.Collections.Generic;
using System.Linq;

namespace Cassandra.ThriftClient.Tests.FunctionalTests.Utils
{
    public static class MoreEnumerable
    {
        public static IEnumerable<TSource> DistinctBy<TSource, TKey>(this IEnumerable<TSource> source, Func<TSource, TKey> keySelector)
        {
            return source.GroupBy(keySelector).Select(g => g.First());
        }
    }
}