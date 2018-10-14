using System;
using System.Collections.Generic;
using System.Linq;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Exceptions;

using Vostok.Logging.Abstractions;
using Vostok.Logging.Abstractions.Extensions;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    internal static class MultigetQueryHelpers
    {
        [NotNull]
        internal static Dictionary<byte[], TValue> EnumerateAllKeysWithPartialFetcher<TValue>(
            [NotNull] List<byte[]> keys,
            [NotNull] Func<List<byte[]>, Dictionary<byte[], TValue>> partialFetcher,
            [CanBeNull] ILog logger = null)
        {
            var keysToFetch = new HashSet<byte[]>(keys, ByteArrayEqualityComparer.Instance);
            var output = new Dictionary<byte[], TValue>();
            var attempts = 0;
            while(keysToFetch.Any())
            {
                var maybePartialOutput = FetchPartialResult(keysToFetch.ToList(), partialFetcher);
                foreach(var item in maybePartialOutput)
                {
                    output.Add(item.Key, item.Value);
                    keysToFetch.Remove(item.Key);
                }

                attempts++;
            }

            if(attempts > 1)
            {
                logger?.Warn($"Enumerate {keys.Count} partitions in {attempts} attempts");
            }

            return output;
        }

        [NotNull]
        private static Dictionary<byte[], TValue> FetchPartialResult<TValue>(
            [NotNull] List<byte[]> keys,
            [NotNull] Func<List<byte[]>, Dictionary<byte[], TValue>> partialFetcher)
        {
            var maybePartialOutput = partialFetcher(keys);
            if(maybePartialOutput.Count == 0)
                throw new CassandraClientInvalidResponseException($"Queried {keys.Count} partitions, Cassandra returned empty result");

            return maybePartialOutput;
        }
    }
}