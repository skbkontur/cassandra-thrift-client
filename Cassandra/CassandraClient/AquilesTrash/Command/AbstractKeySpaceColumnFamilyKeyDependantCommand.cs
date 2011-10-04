using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Abstract class for an AquilesCommand that needs to have a Keyspace, a ColumnFamily and a Key
    /// </summary>
    public abstract class AbstractKeyspaceColumnFamilyKeyDependantCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        /// <summary>
        /// get or set the Key
        /// </summary>
        public byte[] Key
        {
            set;
            get;
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public new virtual void ValidateInput()
        {
            base.ValidateInput();
            if (this.Key == null || this.Key.Length == 0)
            {
                throw new AquilesCommandParameterException("Key must be not null or empty.");
            }
        }
    }
}
