using System;
using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class BatchMutateCommand : AbstractKeyspaceDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            var mutation_map = Translate(Mutations);
            cassandraClient.batch_mutate(mutation_map, GetCassandraConsistencyLevel());
        }

        public override void ValidateInput(ICassandraLogger logger)
        {
            if (Mutations == null)
                throw new AquilesCommandParameterException("No mutations found");

            foreach (var mutationsPerColumnFamily in Mutations)
            {
                var columnFamily = mutationsPerColumnFamily.Key;
                if (String.IsNullOrEmpty(columnFamily))
                    throw new AquilesCommandParameterException("ColumnFamily cannot be null or empty.");

                foreach (var mutationsPerRow in mutationsPerColumnFamily.Value)
                {
                    var rowKey = mutationsPerRow.Key;
                    if (rowKey == null || rowKey.Length == 0)
                        throw new AquilesCommandParameterException("Key cannot be null or empty.");

                    var mutations = mutationsPerRow.Value;
                    if (mutations == null || mutations.Count == 0)
                        throw new AquilesCommandParameterException("No mutations found for ColumnFamily '{0}' over Key '{1}'.", columnFamily, rowKey);

                    foreach (var mutation in mutations)
                        mutation.Validate();
                }
            }
        }

        //from ColumnFamily to KVP<RowKey, Mutation>
        public Dictionary<string, Dictionary<byte[], List<IAquilesMutation>>> Mutations { private get; set; }

        private Dictionary<byte[], Dictionary<string, List<Mutation>>> Translate(Dictionary<string, Dictionary<byte[], List<IAquilesMutation>>> mutations)
        {
            var result = new Dictionary<byte[], Dictionary<string, List<Mutation>>>(new ByteArrayEqualityComparer());
            foreach (var mutationsPerColumnFamily in mutations)
            {
                foreach (var mutationsPerRow in mutationsPerColumnFamily.Value)
                {
                    var mutationList = mutationsPerRow.Value.Select(ModelConverterHelper.Convert<IAquilesMutation, Mutation>).ToList();
                    if (!result.ContainsKey(mutationsPerRow.Key))
                        result.Add(mutationsPerRow.Key, new Dictionary<string, List<Mutation>>());
                    if (!result[mutationsPerRow.Key].ContainsKey(mutationsPerColumnFamily.Key))
                        result[mutationsPerRow.Key].Add(mutationsPerColumnFamily.Key, new List<Mutation>());
                    result[mutationsPerRow.Key][mutationsPerColumnFamily.Key].AddRange(mutationList);
                }
            }
            return result;
        }
    }
}