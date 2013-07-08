using System;
using System.Net;
using System.Net.Sockets;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

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
                try
                {
                    command.Execute(cassandraClient);
                }
                catch(Exception e)
                {
                    logger.Error(string.Format("Команда завершилась неудачей. Время жизни коннекции до этого: {0}", DateTime.UtcNow - CreationDateTime), e);
                    throw;
                }
            }
        }

        public bool Ping()
        {
            lock(lockObject)
            {
                if(bad)
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
                    logger.Error("Error while ping", e);
                    bad = true;
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

        public bool IsAlive { get { return CassandraTransportIsOpen(); } }

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
        private bool bad;

        private readonly string keyspaceName;
        private readonly IPEndPoint ipEndPoint;
        private readonly Apache.Cassandra.Cassandra.Client cassandraClient;
        private readonly Socket socket;
        private readonly ILog logger = LogManager.GetLogger(typeof(ThriftConnection));
        private readonly object lockObject;
        private bool isDisposed;
    }
}