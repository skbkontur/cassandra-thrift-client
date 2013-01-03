using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Simple.Write
{
    public class InsertCommand : KeyspaceColumnFamilyDependantCommandBase
    {
        public InsertCommand(string keyspace, string columnFamily, byte[] rowKey, ConsistencyLevel consistencyLevel, AquilesColumn column)
            : base(keyspace, columnFamily)
        {
            this.rowKey = rowKey;
            this.consistencyLevel = consistencyLevel;
            this.column = column;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            var apacheColumn = ModelConverterHelper.Convert<AquilesColumn, Column>(column);
            var columnParent = BuildColumnParent();
            cassandraClient.insert(rowKey, columnParent, apacheColumn, consistencyLevel);
        }

        private readonly byte[] rowKey;
        private readonly ConsistencyLevel consistencyLevel;
        private readonly AquilesColumn column;
    }
}