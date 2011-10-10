using System;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public abstract class AbstractKeyspaceColumnFamilyDependantCommand : AbstractKeyspaceDependantCommand
    {
        public override void ValidateInput(ICassandraLogger logger)
        {
            if(String.IsNullOrEmpty(ColumnFamily))
                throw new AquilesCommandParameterException("ColumnFamily must be not null or empty.");
        }

        public string ColumnFamily { set; protected get; }

        protected ColumnPath BuildColumnPath(byte[] column)
        {
            var columnPath = new ColumnPath {Column_family = ColumnFamily};
            if(column != null)
                columnPath.Column = column;
            return columnPath;
        }

        protected ColumnParent BuildColumnParent()
        {
            var columnParent = new ColumnParent {Column_family = ColumnFamily};
            return columnParent;
        }
    }
}