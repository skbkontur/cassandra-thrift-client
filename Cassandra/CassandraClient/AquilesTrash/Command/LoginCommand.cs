using System.Collections.Generic;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Log;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Command
{
    public class LoginCommand : AbstractKeyspaceDependantCommand
    {
        public override void Execute(Apache.Cassandra.Cassandra.Client cassandraClient, ICassandraLogger logger)
        {
            AuthenticationRequest authReq = BuildAuthenticationRequest();
            cassandraClient.login(authReq);
        }

        public override void ValidateInput(ICassandraLogger logger)
        {
            if(Credentials == null || Credentials.Count == 0)
                throw new AquilesCommandParameterException("No credential information provided.");
        }

        public Dictionary<string, string> Credentials { private get; set; }

        private AuthenticationRequest BuildAuthenticationRequest()
        {
            return new AuthenticationRequest {Credentials = Credentials};
        }
    }
}