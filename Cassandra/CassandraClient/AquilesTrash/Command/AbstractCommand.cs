using Apache.Cassandra;



namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Abstract Command that adds support for consistency Level over a real command
    /// </summary>
    public abstract class AbstractCommand
    {
        private const AquilesConsistencyLevel DEFAULTCONSISTENCYLEVEL = AquilesConsistencyLevel.QUORUM;

        /// <summary>
        /// Default constructor
        /// </summary>
        protected AbstractCommand()
        {
            this.ConsistencyLevel = DEFAULTCONSISTENCYLEVEL;
        }
        
        /// <summary>
        /// get or set the consistency level required.
        /// <remarks>If you dont know what is this, leave unassigned</remarks>
        /// </summary>
        public AquilesConsistencyLevel ConsistencyLevel
        {
            get;
            set;
        }

        /// <summary>
        /// get the consistencyLevel on Cassandra Thrift structure
        /// </summary>
        protected ConsistencyLevel GetCassandraConsistencyLevel()
        {
            int tempValue = (int) this.ConsistencyLevel;
            return (Apache.Cassandra.ConsistencyLevel)tempValue;

            //switch (this.ConsistencyLevel)
            //{
            //    case AquilesConsistencyLevel.ALL:
            //        return Apache.Cassandra.ConsistencyLevel.ALL;
            //    case AquilesConsistencyLevel.ANY:
            //        return Apache.Cassandra.ConsistencyLevel.ANY;
            //    case AquilesConsistencyLevel.LOCAL_QUORUM:
            //        return Apache.Cassandra.ConsistencyLevel.LOCAL_QUORUM;
            //    case AquilesConsistencyLevel.EACH_QUORUM:
            //        return Apache.Cassandra.ConsistencyLevel.EACH_QUORUM;
            //    case AquilesConsistencyLevel.ONE:
            //        return Apache.Cassandra.ConsistencyLevel.ONE;
            //    case AquilesConsistencyLevel.QUORUM:
            //        return Apache.Cassandra.ConsistencyLevel.QUORUM;
            //    default:
            //        throw new AquilesException("Unsupported consistency level.");
            //}
        }

        /// <summary>
        /// Indicates if this command applies only to a keyspace
        /// </summary>
        public virtual bool isKeyspaceDependant
        {
            get { return false; }
        }
    }
}
