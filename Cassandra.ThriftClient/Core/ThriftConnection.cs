using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;

using Apache.Cassandra;

using SkbKontur.Cassandra.ThriftClient.Abstractions;
using SkbKontur.Cassandra.ThriftClient.Exceptions;
using SkbKontur.Cassandra.TimeBasedUuid;

using Thrift.Protocol;
using Thrift.Transport;

using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Core
{
    internal class ThriftConnection : IThriftConnection
    {
        public ThriftConnection(int timeout, IPEndPoint ipEndPoint, string keyspaceName, Credentials credentials, ILog logger)
        {
            isDisposed = false;
            isAlive = true;
            this.ipEndPoint = ipEndPoint;
            this.keyspaceName = keyspaceName;
            this.logger = logger;
            this.credentials = credentials;
            var address = ipEndPoint.Address.ToString();
            var port = ipEndPoint.Port;
            var tsocket = timeout == 0 ? new TSocket(address, port) : new TSocket(address, port, timeout);
            var socket = tsocket.TcpClient.Client;
            socket.NoDelay = true;
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, 1);
            var transport = new TFramedTransport(tsocket);
            cassandraClient = new Apache.Cassandra.Cassandra.Client(new TBinaryProtocol(transport));
            creationTimestamp = Timestamp.Now;
            OpenTransport();
        }

        public void Dispose()
        {
            if (isDisposed)
                return;
            isDisposed = true;
            CloseTransport();
        }

        public void ExecuteCommand(ICommand command)
        {
            lock (locker)
            {
                if (!isAlive)
                {
                    var e = new DeadConnectionException();
                    logger.Error(e, "Взяли дохлую коннекцию. Время жизни коннекции до этого: {0}", Timestamp.Now - creationTimestamp);
                    throw e;
                }
                command.Execute(cassandraClient, logger);
            }
        }

        public bool IsAlive => isAlive && CassandraTransportIsOpen() && Ping();

        public override string ToString()
        {
            return $"ThriftConnection[EndPoint='{ipEndPoint}' KeyspaceName='{keyspaceName}']";
        }

        private bool Ping()
        {
            lock (locker)
            {
                if (!isAlive)
                    return false;
                if (lastSuccessPingTimestamp != null && Timestamp.Now - lastSuccessPingTimestamp < TimeSpan.FromMinutes(1))
                    return true;
                try
                {
                    cassandraClient.describe_cluster_name();
                    lastSuccessPingTimestamp = Timestamp.Now;
                }
                catch (Exception e)
                {
                    logger.Error(e, "Error while ping");
                    isAlive = false;
                    return false;
                }
                return true;
            }
        }

        private bool CassandraTransportIsOpen()
        {
            try
            {
                return cassandraClient.InputProtocol.Transport.IsOpen && cassandraClient.OutputProtocol.Transport.IsOpen;
            }
            catch (Exception)
            {
                return false;
            }
        }

        private void OpenTransport()
        {
            lock (locker)
            {
                cassandraClient.InputProtocol.Transport.Open();
                if (!cassandraClient.InputProtocol.Transport.Equals(cassandraClient.OutputProtocol.Transport))
                    cassandraClient.OutputProtocol.Transport.Open();

                WithCloseTransportOnError(Login);
                WithCloseTransportOnError(SetKeyspace);
            }
        }

        private void WithCloseTransportOnError(Action action)
        {
            try
            {
                action();
            }
            catch (Exception e)
            {
                logger.Error(e, "Error occured while opening thrift connection. Will try to close open transports.");
                try
                {
                    DoCloseTransport();
                }
                catch (Exception closeException)
                {
                    logger.Error(closeException, "Error occured while closing connection's transports.");
                }
                throw;
            }
        }

        private void Login()
        {
            if (credentials != null)
                cassandraClient.login(new AuthenticationRequest(new Dictionary<string, string>
                    {
                        ["username"] = credentials.Username,
                        ["password"] = credentials.Password,
                    }));
        }

        private void SetKeyspace()
        {
            if (!string.IsNullOrEmpty(keyspaceName))
                cassandraClient.set_keyspace(keyspaceName);
        }

        private void CloseTransport()
        {
            lock (locker)
            {
                DoCloseTransport();
            }
        }

        private void DoCloseTransport()
        {
            cassandraClient.InputProtocol.Transport.Close();
            if (!cassandraClient.InputProtocol.Transport.Equals(cassandraClient.OutputProtocol.Transport))
                cassandraClient.OutputProtocol.Transport.Close();
        }

        private Timestamp lastSuccessPingTimestamp;
        private bool isAlive;

        private readonly string keyspaceName;
        private readonly ILog logger;
        private readonly Credentials credentials;
        private readonly IPEndPoint ipEndPoint;
        private readonly Apache.Cassandra.Cassandra.Client cassandraClient;
        private readonly Timestamp creationTimestamp;
        private readonly object locker = new object();
        private bool isDisposed;
    }
}