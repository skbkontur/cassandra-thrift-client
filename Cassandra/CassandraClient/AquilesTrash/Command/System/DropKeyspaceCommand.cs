using System;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System
{
    public class DropKeyspaceCommand : AbstractCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = cassandraClient.system_drop_keyspace(Keyspace);
        }

        public override void ValidateInput()
        {
            if(String.IsNullOrEmpty(Keyspace))
                throw new AquilesCommandParameterException("Keyspace cannot be null or empty.");
        }

        public string Keyspace { private get; set; }
        public string Output { get; private set; }
        public override bool IsFierce { get { return true; } }
    }
}