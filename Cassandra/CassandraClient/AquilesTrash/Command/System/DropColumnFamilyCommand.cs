using System;

using Apache.Cassandra;

using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command.System
{
    public class DropColumnFamilyCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
        {
            Output = cassandraClient.system_drop_column_family(ColumnFamily);
        }

        public string Output { get; private set; }
        public override bool IsFierce { get { return true; } }
    }
}