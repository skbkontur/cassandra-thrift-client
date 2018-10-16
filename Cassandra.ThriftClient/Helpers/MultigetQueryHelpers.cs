using System;
using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using JetBrains.Annotations;

using SKBKontur.Cassandra.CassandraClient.Exceptions;

using Vostok.Logging.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Helpers
{
    internal class MultigetQueryHelpers
    {
        private readonly string keyspace;
        private readonly string columnFamily;
        private readonly ConsistencyLevel? consistencyLevel;

        internal MultigetQueryHelpers(string keyspace, string columnFamily, ConsistencyLevel? consistencyLevel)
        {
            this.keyspace = keyspace;
            this.columnFamily = columnFamily;
            this.consistencyLevel = consistencyLevel;
        }

        [NotNull]
        internal Dictionary<byte[], TValue> EnumerateAllKeysWithPartialFetcher<TValue>(
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
                logger?.Warn($"Query with parameters {QueryParameters} enumerates {keys.Count} partitions in {attempts} attempts");
            }

            return output;
        }

        [NotNull]
        private Dictionary<byte[], TValue> FetchPartialResult<TValue>(
            [NotNull] List<byte[]> keys,
            [NotNull] Func<List<byte[]>, Dictionary<byte[], TValue>> partialFetcher)
        {
            var maybePartialOutput = partialFetcher(keys);
            if(maybePartialOutput.Count == 0)
                throw new CassandraClientInvalidResponseException($"Queried {keys.Count} partitions with parameters {QueryParameters}, Cassandra returned empty result");

            return maybePartialOutput;
        }

        private string QueryParameters => $"[keyspace='{keyspace}', columnFamily='{columnFamily}', consistencyLevel={consistencyLevel}]";
    }
}