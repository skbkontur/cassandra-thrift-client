using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base
{
    public abstract class KeyspaceColumnFamilyDependantCommandBase : KeyspaceDependantCommandBase
    {
        protected KeyspaceColumnFamilyDependantCommandBase(string keyspace, string columnFamily)
            : base(keyspace)
        {
            this.columnFamily = columnFamily;
        }

        public override CommandContext CommandContext
        {
            get
            {
                return new CommandContext
                    {
                        KeyspaceName = keyspace,
                        ColumnFamilyName = columnFamily
                    };
            }
        }

        protected ColumnPath BuildColumnPath(byte[] column)
        {
            var columnPath = new ColumnPath {Column_family = columnFamily};
            if(column != null)
                columnPath.Column = column;
            return columnPath;
        }

        protected ColumnParent BuildColumnParent()
        {
            var columnParent = new ColumnParent {Column_family = columnFamily};
            return columnParent;
        }

        protected readonly string columnFamily;
    }
}