using Apache.Cassandra;

using SkbKontur.Cassandra.ThriftClient.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Commands.Base
{
    internal abstract class KeyspaceColumnFamilyDependantCommandBase : KeyspaceDependantCommandBase
    {
        protected KeyspaceColumnFamilyDependantCommandBase(string keyspace, string columnFamily)
            : base(keyspace)
        {
            this.columnFamily = columnFamily;
        }

        public override CommandContext CommandContext => new CommandContext
            {
                KeyspaceName = keyspace,
                ColumnFamilyName = columnFamily
            };

        protected ColumnPath BuildColumnPath(byte[] column)
        {
            var columnPath = new ColumnPath {Column_family = columnFamily};
            if (column != null)
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