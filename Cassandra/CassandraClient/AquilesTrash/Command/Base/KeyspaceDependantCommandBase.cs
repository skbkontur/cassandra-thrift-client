using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base
{
    public abstract class KeyspaceDependantCommandBase : CommandBase
    {
        protected KeyspaceDependantCommandBase(string keyspace)
        {
            this.keyspace = keyspace;
        }

        public override CommandContext CommandContext
        {
            get
            {
                return new CommandContext
                    {
                        KeyspaceName = keyspace
                    };
            }
        }

        protected readonly string keyspace;
    }
}