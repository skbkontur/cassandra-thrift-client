using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    public abstract class AbstractKeyspaceColumnFamilyKeyDependantCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        public override void ValidateInput()
        {
            base.ValidateInput();
            if(Key == null || Key.Length == 0)
                throw new AquilesCommandParameterException("Key must be not null or empty.");
        }

        public byte[] Key { set; protected get; }
    }
}