using System;
using System.Net;
using System.Net.Sockets;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Exceptions;

using Thrift.Protocol;
using Thrift.Transport;

using log4net;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public class ThriftConnection
    {
        public ThriftConnection(int timeout, IPEndPoint ipEndPoint, string keyspaceName)
        {
            isDisposed = false;
            IsAlive = true;
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

        public void ExecuteCommand(ICommand command)
        {
            lock(lockObject)
            {
                if(!isAlive)
                {
                    var e = new DeadConnectionException();
                    logger.Error(string.Format("Взяли дохлую коннекцию. Время жизни коннекции до этого: {0}", DateTime.UtcNow - CreationDateTime), e);
                    throw e;
                }
                try
                {
                    command.Execute(cassandraClient);
                }
                catch(Exception e)
                {
                    logger.Error(string.Format("Команда завершилась неудачей. Время жизни коннекции до этого: {0}", DateTime.UtcNow - CreationDateTime), e);
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
                    cassandraClient.describe_cluster_name();
                }
                catch(Exception e)
                {
                    logger.Error("Error while ping", e);
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
        private readonly ILog logger = LogManager.GetLogger(typeof(ThriftConnection));
        private readonly object lockObject;
        private bool isDisposed;
    }
}