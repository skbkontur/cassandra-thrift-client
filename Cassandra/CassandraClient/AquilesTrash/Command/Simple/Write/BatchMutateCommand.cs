using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Write
{
    public class BatchMutateCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public BatchMutateCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, Dictionary<string, Dictionary<byte[], List<IAquilesMutation>>> mutations)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.mutations = mutations;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var mutation_map = TranslateMutations();
            cassandraClient.batch_mutate(mutation_map, consistencyLevel);
        }

        private Dictionary<byte[], Dictionary<string, List<Mutation>>> TranslateMutations()
        {
            var result = new Dictionary<byte[], Dictionary<string, List<Mutation>>>(ByteArrayEqualityComparer.SimpleComparer);
            foreach(var mutationsPerColumnFamily in mutations)
            {
                foreach(var mutationsPerRow in mutationsPerColumnFamily.Value)
                {
                    var mutationList = mutationsPerRow.Value.Select(ModelConverterHelper.Convert<IAquilesMutation, Mutation>).ToList();
                    if(!result.ContainsKey(mutationsPerRow.Key))
                        result.Add(mutationsPerRow.Key, new Dictionary<string, List<Mutation>>());
                    if(!result[mutationsPerRow.Key].ContainsKey(mutationsPerColumnFamily.Key))
                        result[mutationsPerRow.Key].Add(mutationsPerColumnFamily.Key, new List<Mutation>());
                    result[mutationsPerRow.Key][mutationsPerColumnFamily.Key].AddRange(mutationList);
                }
            }
            return result;
        }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly Dictionary<string, Dictionary<byte[], List<IAquilesMutation>>> mutations;
    }
}