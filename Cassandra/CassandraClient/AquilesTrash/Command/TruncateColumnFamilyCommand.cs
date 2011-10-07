
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to truncate a ColumnFamily within a Keyspace
    /// </summary>
    public class TruncateColumnFamilyCommand : AbstractKeyspaceColumnFamilyDependantCommand
    {
        /// <summary>
        /// Executes a "truncate" over the connection.
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient)
        {
            cassandraClient.truncate(this.ColumnFamily);
        }

        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public override void ValidateInput()
        {
            base.ValidateInput();
        }
        
    }
}
