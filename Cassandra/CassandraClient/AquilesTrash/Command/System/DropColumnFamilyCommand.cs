using System;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System
{
    public class DropColumnFamilyCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            Output = cassandraClient.system_drop_column_family(ColumnFamily);
        }

        public string Output { get; private set; }
        public override bool IsFierce { get { return true; } }
    }
}