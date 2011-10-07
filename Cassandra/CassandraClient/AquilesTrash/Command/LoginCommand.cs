using System.Collections.Generic;

using Apache.Cassandra;

using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    public class LoginCommand : AbstractKeyspaceDependantCommand
    {
        public override void Execute(Cassandra.Client cassandraClient)
        {
            AuthenticationRequest authReq = BuildAuthenticationRequest();
            cassandraClient.login(authReq);
        }

        public override void ValidateInput()
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