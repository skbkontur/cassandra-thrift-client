using System;
using System.Net;

using CassandraClient.Abstractions;
using CassandraClient.Clusters;
using CassandraClient.Core.Pools;
using CassandraClient.Exceptions;

namespace CassandraClient.Core
{
    public class CommandExecuter : ICommandExecuter
    {
        public CommandExecuter(IClusterConnectionPool clusterConnectionPool, IEndpointManager endpointManager, ICassandraClusterSettings settings, CassandraClientExceptionTypeRecognizer recognizer)
        {
            this.clusterConnectionPool = clusterConnectionPool;
            this.endpointManager = endpointManager;
            this.settings = settings;
            this.recognizer = recognizer;
            foreach (var ipEndPoint in settings.Endpoints)
                this.endpointManager.Register(ipEndPoint);
        }

        public void Execute(ICommand command)
        {
            ValidationResult validationResult = command.Validate();
            if (validationResult.Status != ValidationStatus.Ok)
                throw new CassandraClientInvalidRequestException(validationResult.Message);
            for (int i = 0; i < settings.Attempts; ++i)
            {
                IPEndPoint endpoint = endpointManager.GetEndPoint();
                try
                {
                    using (var thriftConnection = clusterConnectionPool.BorrowConnection(endpoint, command.Keyspace))
                        thriftConnection.ExecuteCommand(command);
                    endpointManager.Good(endpoint);
                    return;
                }
                catch (Exception e)
                {
                    string message = string.Format("An error occured while executing cassandra command '{0}'", command.GetType());
                    var exception = CassandraExceptionTransformer.Transform(e, message);
                    if (!recognizer.IsExceptionRetryable(exception))
                        throw exception;
                    endpointManager.Bad(endpoint);
                }
            }
        }

        private readonly IClusterConnectionPool clusterConnectionPool;
        private readonly IEndpointManager endpointManager;
        private readonly ICassandraClusterSettings settings;
        private readonly CassandraClientExceptionTypeRecognizer recognizer;
    }
}