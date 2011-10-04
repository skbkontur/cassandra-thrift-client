using System.Collections.Generic;

using Apache.Cassandra;
using CassandraClient.AquilesTrash.Exceptions;

namespace CassandraClient.AquilesTrash.Command
{
    /// <summary>
    /// Command to login to a cluster to operate against the given Keyspace
    /// </summary>
    public class LoginCommand : AbstractKeyspaceDependantCommand, IAquilesCommand
    {
        /// <summary>
        /// get or set the Credential information
        /// <remarks>It's a key-value map</remarks>
        /// </summary>
        public Dictionary<string, string> Credentials
        {
            get;
            set;
        }

        #region IAquilesCommand Members
        /// <summary>
        /// Executes a "login" over the connection.
        /// <remarks>In case the login fails, posible login exceptions are AuthenticationException, AuthorizationException</remarks>
        /// </summary>
        /// <param name="cassandraClient">opened Thrift client</param>
        public void Execute(Cassandra.Client cassandraClient)
        {
            AuthenticationRequest authReq = this.BuildAuthenticationRequest();
            cassandraClient.login(authReq);
        }
        /// <summary>
        /// Validate the input parameters. 
        /// Throws <see cref="AquilesCommandParameterException"/>  in case there is some malformed or missing input parameters
        /// </summary>
        public void ValidateInput()
        {
            if ((this.Credentials == null) || (this.Credentials != null && this.Credentials.Count > 0))
            {
                throw new AquilesCommandParameterException("No credential information provided.");
            }
        }
        #endregion

        private AuthenticationRequest BuildAuthenticationRequest()
        {
            AuthenticationRequest req = new AuthenticationRequest();
            req.Credentials = this.Credentials;

            return req;
        }
    }
}
