using System;
using System.Collections.Generic;

using CassandraClient.AquilesTrash.Model;
using Apache.Cassandra;

using System.Globalization;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to batch actions over a Keyspace of a Cluster.
    /// Posible actions are:
    ///     - insert
    ///     - delete
    /// </summary>
    public class BatchMutateCommand : AbstractKeyspaceDependantCommand, IAquilesCommand
    {
        /// <summary>
        /// get or set Mutation actions to be applied
        /// </summary>
        public Dictionary<byte[], Dictionary<string, List<IAquilesMutation>>> Mutations
        {
            get;
            set;
        }

        /// <summary>
        /// Executes a "batch_mutate" over the connection. No return values
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public override void Execute(Cassandra.Client cassandraClient)
        {
            Dictionary<byte[], Dictionary<string, List<Mutation>>> mutation_map = this.translate(this.Mutations);
            cassandraClient.batch_mutate(mutation_map, this.GetCassandraConsistencyLevel());
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public override void ValidateInput()
        {
            if (this.Mutations != null)
            {
                byte[] key;
                string columnFamily;
                List<IAquilesMutation> mutations;
                Dictionary<string, List<IAquilesMutation>> mutationsOverAKey;
                Dictionary<string, List<IAquilesMutation>>.Enumerator mutationsOverAKeyEnumerator;
                Dictionary<byte[], Dictionary<string, List<IAquilesMutation>>>.Enumerator keysEnumerator = this.Mutations.GetEnumerator();
                while (keysEnumerator.MoveNext())
                {
                    key = keysEnumerator.Current.Key;
                    if (key != null && key.Length != 0)
                    {
                        mutationsOverAKey = keysEnumerator.Current.Value;
                        mutationsOverAKeyEnumerator = mutationsOverAKey.GetEnumerator();
                        while (mutationsOverAKeyEnumerator.MoveNext())
                        {
                            columnFamily = mutationsOverAKeyEnumerator.Current.Key;
                            if (!String.IsNullOrEmpty(columnFamily))
                            {
                                mutations = mutationsOverAKeyEnumerator.Current.Value;
                                if ((mutations == null) || (mutations != null && mutations.Count == 0))
                                {
                                    throw new AquilesCommandParameterException(String.Format(CultureInfo.CurrentCulture, "No mutations found for ColumnFamily '{0}' over Key '{1}'.", columnFamily, key));
                                }
                                else
                                {
                                    foreach (IAquilesMutation mutation in mutations)
                                    {
                                        mutation.Validate();
                                    }
                                }
                            }
                            else
                            {
                                throw new AquilesCommandParameterException("ColumnFamily cannot be null or empty.");
                            }
                        }
                    }
                    else
                    {
                        throw new AquilesCommandParameterException("Key cannot be null or empty.");
                    }
                }
            }
            else
            {
                throw new AquilesCommandParameterException("No mutations found");
            }
        }

        

        private Dictionary<byte[], Dictionary<string, List<Mutation>>> translate(Dictionary<byte[], Dictionary<string, List<IAquilesMutation>>> mutations)
        {
            Dictionary<string, List<IAquilesMutation>>.Enumerator mutationOverKeyEnumerator;
            Dictionary<byte[], Dictionary<string, List<Mutation>>> cassandraKeyColumnFamilyMutationMap = new Dictionary<byte[], Dictionary<string, List<Mutation>>>();
            Dictionary<string, List<Mutation>> cassandraColumnFamilyMutationMap = null;
            List<Mutation> mutationList = null;
            Dictionary<byte[], Dictionary<string, List<IAquilesMutation>>>.Enumerator mutationEnumerator = mutations.GetEnumerator();
            while (mutationEnumerator.MoveNext())
            {
                cassandraColumnFamilyMutationMap = new Dictionary<string, List<Mutation>>();
                mutationOverKeyEnumerator = mutationEnumerator.Current.Value.GetEnumerator();
                while (mutationOverKeyEnumerator.MoveNext())
                {
                    mutationList = new List<Mutation>(mutationOverKeyEnumerator.Current.Value.Count);
                    foreach (IAquilesMutation aquilesMutation in mutationOverKeyEnumerator.Current.Value)
                    {
                        mutationList.Add(ModelConverterHelper.Convert<IAquilesMutation,Mutation>(aquilesMutation));
                    }
                    cassandraColumnFamilyMutationMap.Add(mutationOverKeyEnumerator.Current.Key, mutationList);
                }
                cassandraKeyColumnFamilyMutationMap.Add(mutationEnumerator.Current.Key, cassandraColumnFamilyMutationMap);
            }

            return cassandraKeyColumnFamilyMutationMap;
        }
    }
}