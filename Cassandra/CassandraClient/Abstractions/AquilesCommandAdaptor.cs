using System;

using Apache.Cassandra;

using Aquiles;

namespace CassandraClient.Abstractions
{
/*todo Срочно порвать эту херь*/
    public class AquilesCommandAdaptor : ICommand
    {
        public readonly IAquilesCommand command;

        public AquilesCommandAdaptor(IAquilesCommand command, string keyspace = null)
        {
            Keyspace = keyspace;
            this.command = command;
        }

        public void Execute(Cassandra.Client client)
        {
            command.Execute(client);
        }

        public ValidationResult Validate()
        {
            return ValidationResult.Ok();
            throw new NotImplementedException();
        }

        
        public string Keyspace { get; private set; }
    }
}