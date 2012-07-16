using System;
using System.Net;
using System.Net.Sockets;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Exceptions;
using SKBKontur.Cassandra.CassandraClient.Log;

using Thrift.Protocol;
using Thrift.Transport;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public class ThriftConnection
    {
        public ThriftConnection(int timeout, IPEndPoint ipEndPoint, string keyspaceName, ICassandraLogManager logManager)
        {
            isDisposed = false;
            IsAlive = true;
            logger = logManager.GetLogger(GetType());
            this.ipEndPoint = ipEndPoint;
            this.keyspaceName = keyspaceName;
            string address = ipEndPoint.Address.ToString();
            int port = ipEndPoint.Port;
            TSocket tsocket = timeout == 0 ? new TSocket(address, port) : new TSocket(address, port, timeout);
            socket = tsocket.TcpClient.Client;
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

        public void ExecuteCommand(ICommand command, ICassandraLogger logger)
        {
            lock(lockObject)
            {
                if(!isAlive)
                {
                    var e = new DeadConnectionException();
                    logger.Error(e, "Взяли дохлую коннекцию. Время жизни коннекции до этого: {0}", DateTime.UtcNow - CreationDateTime);
                    throw e;
                }
                try
                {
                    command.Execute(cassandraClient, logger);
                }
                catch(Exception e)
                {
                    logger.Error(e, "Команда завершилась неудачей. Время жизни коннекции до этого: {0}", DateTime.UtcNow - CreationDateTime);
                    IsAlive = false;
                    throw;
                }
            }
        }

        public bool Ping()
        {
            lock(lockObject)
            {
                try
                {
                    cassandraClient.describe_version();
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

        public override string ToString()
        {
            return string.Format("ThriftConnection[EndPoint='{0}' KeyspaceName='{1}']", ipEndPoint, keyspaceName);
        }

        public DateTime CreationDateTime { get; private set; }

        public bool IsAlive { get { return isAlive && CassandraTransportIsOpen(); } set { isAlive = value; } }

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

        private readonly string keyspaceName;
        private readonly IPEndPoint ipEndPoint;
        private volatile bool isAlive;
        private readonly Apache.Cassandra.Cassandra.Client cassandraClient;
        private readonly Socket socket;
        private ICassandraLogger logger;
        private readonly object lockObject;
        private bool isDisposed;
    }
}