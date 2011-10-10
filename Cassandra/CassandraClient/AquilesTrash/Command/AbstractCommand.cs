using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public abstract class AbstractCommand : IAquilesCommand
    {
        protected AbstractCommand()
        {
            ConsistencyLevel = defaultConsistencyLevel;
        }

        public AquilesConsistencyLevel ConsistencyLevel { get; set; }

        protected ConsistencyLevel GetCassandraConsistencyLevel()
        {
            var tempValue = (int)ConsistencyLevel;
            return (ConsistencyLevel)tempValue;
        }

        private const AquilesConsistencyLevel defaultConsistencyLevel = AquilesConsistencyLevel.QUORUM;
        public abstract void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger);

        public virtual void ValidateInput(ICassandraLogger logger) { }

        public virtual bool IsFierce { get { return false; } }
    }
}