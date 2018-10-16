using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.Commands.Base;
using SKBKontur.Cassandra.CassandraClient.Helpers;

using Vostok.Logging.Abstractions;

using ConsistencyLevel = Apache.Cassandra.ConsistencyLevel;

namespace SKBKontur.Cassandra.CassandraClient.Commands.Simple.Write
{
    internal class BatchMutateCommand : KeyspaceColumnFamilyDependantCommandBase, ISimpleCommand
    {
        public BatchMutateCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, Dictionary<string, Dictionary<byte[], List<IMutation>>> mutations)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.mutations = mutations;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            var mutationMap = TranslateMutations();
            cassandraClient.batch_mutate(mutationMap, consistencyLevel);
        }

        private Dictionary<byte[], Dictionary<string, List<Mutation>>> TranslateMutations()
        {
            var result = new Dictionary<byte[], Dictionary<string, List<Mutation>>>(ByteArrayEqualityComparer.Instance);
            foreach (var mutationsPerColumnFamily in mutations)
            {
                foreach (var mutationsPerRow in mutationsPerColumnFamily.Value)
                {
                    var mutationList = mutationsPerRow.Value.Select(mutation => mutation.ToCassandraMutation()).ToList();
                    if (!result.ContainsKey(mutationsPerRow.Key))
                        result.Add(mutationsPerRow.Key, new Dictionary<string, List<Mutation>>());
                    if (!result[mutationsPerRow.Key].ContainsKey(mutationsPerColumnFamily.Key))
                        result[mutationsPerRow.Key].Add(mutationsPerColumnFamily.Key, new List<Mutation>());
                    result[mutationsPerRow.Key][mutationsPerColumnFamily.Key].AddRange(mutationList);
                }
            }
            return result;
        }

        public int QueriedPartitionsCount { get { return mutations.Sum(columnFamilyMutations => columnFamilyMutations.Value.Count); } }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly Dictionary<string, Dictionary<byte[], List<IMutation>>> mutations;
    }
}