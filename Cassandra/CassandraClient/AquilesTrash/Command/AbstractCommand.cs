using Apache.Cassandra;

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
        public abstract void Execute(Apache.Cassandra.Cassandra.Client cassandraClient);

        public virtual void ValidateInput(){}

        public virtual bool IsFierce { get { return false; } }
    }
}