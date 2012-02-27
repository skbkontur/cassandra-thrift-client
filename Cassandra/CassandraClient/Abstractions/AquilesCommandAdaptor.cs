using System;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    public class AquilesCommandAdaptor : ICommand
    {
        public readonly IAquilesCommand command;

        public AquilesCommandAdaptor(IAquilesCommand command, string keyspace = null)
        {
            Keyspace = keyspace;
            this.command = command;
        }

        public void Execute(Apache.Cassandra.Cassandra.Client client, ICassandraLogger logger)
        {
            command.Execute(client, logger);
        }

        public ValidationResult Validate(ICassandraLogger logger)
        {
            //todo что-то решить с валидациями
            return ValidationResult.Ok();
        }


        
        public string Keyspace { get; private set; }
        public virtual bool IsFierce { get { return command.IsFierce; } }

        public Type GetCommandType()
        {
            return command.GetType();
        }
    }
}