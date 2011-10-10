using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public abstract class AbstractKeyspaceColumnFamilyKeyDependantCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        public override void ValidateInput(ICassandraLogger logger)
        {
            base.ValidateInput(logger);
            if(Key == null || Key.Length == 0)
                throw new AquilesCommandParameterException("Key must be not null or empty.");
        }

        public byte[] Key { set; protected get; }
    }
}