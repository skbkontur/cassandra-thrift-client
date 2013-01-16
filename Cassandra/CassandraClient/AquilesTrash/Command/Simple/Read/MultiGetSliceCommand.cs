﻿using System.Collections.Generic;
using System.Linq;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions.Internal;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

using SlicePredicate = SKBKontur.Cassandra.CassandraClient.Abstractions.Internal.SlicePredicate;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Read
{
    internal class MultiGetSliceCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public MultiGetSliceCommand(string keyspace, string columnFamily, ConsistencyLevel consistencyLevel, List<byte[]> keys, SlicePredicate predicate)
            : base(keyspace, columnFamily)
        {
            this.consistencyLevel = consistencyLevel;
            this.keys = keys;
            this.predicate = predicate;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var output = cassandraClient.multiget_slice(keys, BuildColumnParent(), predicate.ToCassandraSlicePredicate(), consistencyLevel);
            BuildOut(output);
        }

        public Dictionary<byte[], List<AquilesColumn>> Output { get; private set; }

        private void BuildOut(Dictionary<byte[], List<ColumnOrSuperColumn>> output)
        {
            Output = new Dictionary<byte[], List<AquilesColumn>>();
            foreach(var outputKeyValuePair in output)
            {
                var columnOrSuperColumnList = outputKeyValuePair.Value.Select(x => x.Column)
                    .Select(ModelConverterHelper.Convert<AquilesColumn, Column>)
                    .ToList();
                Output.Add(outputKeyValuePair.Key, columnOrSuperColumnList);
            }
        }

        private readonly ConsistencyLevel consistencyLevel;
        private readonly List<byte[]> keys;
        private readonly SlicePredicate predicate;
    }
}