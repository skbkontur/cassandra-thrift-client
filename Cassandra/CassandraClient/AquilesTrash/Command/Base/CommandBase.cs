using SKBKontur.Cassandra.CassandraClient.Abstractions;

using log4net;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command.Base
{
    public abstract class CommandBase : ICommand
    {
        public abstract void Execute(Apache.Cassandra.Cassandra.Client client);
        public string Name { get { return GetType().Name; } }
        public virtual bool IsFierce { get { return false; } }
        public virtual CommandContext CommandContext { get { return new CommandContext(); } }
        protected readonly ILog logger = LogManager.GetLogger(typeof(CommandBase));
    }
}