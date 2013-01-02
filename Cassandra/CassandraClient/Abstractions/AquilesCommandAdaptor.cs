using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class AquilesCommandAdaptor : ICommand
    {
        public AquilesCommandAdaptor(IAquilesCommand command, string keyspace = null)
        {
            Keyspace = keyspace;
            this.command = command;
        }

        public void Execute(Apache.Cassandra.Cassandra.Client client)
        {
            command.Execute(client);
        }

        public ValidationResult Validate()
        {
            //todo что-то решить с валидациями
            return ValidationResult.Ok();
        }

        public string Keyspace { get; private set; }
        public string Name { get { return command.GetType().Name; } }
        public virtual bool IsFierce { get { return command.IsFierce; } }
        public readonly IAquilesCommand command;
    }
}