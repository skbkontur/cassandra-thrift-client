﻿using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.System.Write
{
    public class UpdateKeyspaceCommand : CommandBase
    {
        public UpdateKeyspaceCommand(Keyspace keyspaceDefinition)
        {
            this.keyspaceDefinition = keyspaceDefinition;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            Output = cassandraClient.system_update_keyspace(keyspaceDefinition.ToCassandraKsDef());
        }

        public string Output { get; private set; }
        public override bool IsFierce { get { return true; } }
        private readonly Keyspace keyspaceDefinition;
    }
}