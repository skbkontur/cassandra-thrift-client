using System;
using System.Net;
using System.Net.Sockets;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

using Thrift.Protocol;
using Thrift.Transport;

using Vostok.Logging;
using Vostok.Logging.Extensions;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    internal class ThriftConnection : IThriftConnection
    {
        public ThriftConnection(int timeout, IPEndPoint ipEndPoint, string keyspaceName, ILog logger)
        {
            isDisposed = false;
            isAlive = true;
            this.ipEndPoint = ipEndPoint;
            this.keyspaceName = keyspaceName;
            this.logger = logger;
            var address = ipEndPoint.Address.ToString();
            var port = ipEndPoint.Port;
            var tsocket = timeout == 0 ? new TSocket(address, port) : new TSocket(address, port, timeout);
            var socket = tsocket.TcpClient.Client;
            socket.NoDelay = true;
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, 1);
            var transport = new TFramedTransport(tsocket);
            cassandraClient = new Apache.Cassandra.Cassandra.Client(new TBinaryProtocol(transport));
            lockObject = new object();
            CreationDateTime = DateTime.UtcNow;
            OpenTransport();
        }

        public void Dispose()
        {
            if(isDisposed)
                return;
            isDisposed = true;
            CloseTransport();
        }

        public void ExecuteCommand(ICommand command)
        {
            lock(lockObject)
            {
                if(!isAlive)
                {
                    var e = new DeadConnectionException();
                    logger.Error(e, "Взяли дохлую коннекцию. Время жизни коннекции до этого: {0}", DateTime.UtcNow - CreationDateTime);
                    throw e;
                }
                command.Execute(cassandraClient);
            }
        }

        public DateTime CreationDateTime { get; private set; }

        public bool IsAlive { get { return isAlive && CassandraTransportIsOpen() && Ping(); } }

        public override string ToString()
        {
            return string.Format("ThriftConnection[EndPoint='{0}' KeyspaceName='{1}']", ipEndPoint, keyspaceName);
        }

        private bool Ping()
        {
            lock(lockObject)
            {
                if(!isAlive)
                    return false;
                if(lastSuccessPingDateTime.HasValue && DateTime.UtcNow - lastSuccessPingDateTime.Value < TimeSpan.FromMinutes(1))
                    return true;
                try
                {
                    cassandraClient.describe_cluster_name();
                    lastSuccessPingDateTime = DateTime.UtcNow;
                }
                catch(Exception e)
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
                return (cassandraClient.InputProtocol.Transport.IsOpen && cassandraClient.OutputProtocol.Transport.IsOpen);
            }
            catch(Exception)
            {
                return false;
            }
        }

        private void OpenTransport()
        {
            lock(lockObject)
            {
                cassandraClient.InputProtocol.Transport.Open();
                if(!cassandraClient.InputProtocol.Transport.Equals(cassandraClient.OutputProtocol.Transport))
                    cassandraClient.OutputProtocol.Transport.Open();
                if(!string.IsNullOrEmpty(keyspaceName))
                    cassandraClient.set_keyspace(keyspaceName);
            }
        }

        private void CloseTransport()
        {
            lock(lockObject)
            {
                cassandraClient.InputProtocol.Transport.Close();
                if(!cassandraClient.InputProtocol.Transport.Equals(cassandraClient.OutputProtocol.Transport))
                    cassandraClient.OutputProtocol.Transport.Close();
            }
        }

        private DateTime? lastSuccessPingDateTime;
        private bool isAlive;

        private readonly string keyspaceName;
        private readonly ILog logger;
        private readonly IPEndPoint ipEndPoint;
        private readonly Apache.Cassandra.Cassandra.Client cassandraClient;
        private readonly object lockObject;
        private bool isDisposed;
    }
}