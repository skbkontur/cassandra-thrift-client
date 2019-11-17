using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Commands.Base;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Commands.System.Write
{
    internal class UpdateColumnFamilyCommand : KeyspaceDependantCommandBase, IFierceCommand
    {
        public UpdateColumnFamilyCommand(string keyspace, ColumnFamily columnFamilyDefinition)
            : base(keyspace)
        {
            this.columnFamilyDefinition = columnFamilyDefinition;
        }

        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ILog logger)
        {
            Output = cassandraClient.system_update_column_family(columnFamilyDefinition.ToCassandraCfDef(keyspace));
        }

        public string Output { get; private set; }
        private readonly ColumnFamily columnFamilyDefinition;
    }
}