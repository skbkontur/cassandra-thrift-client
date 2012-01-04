using System.Net;
using System.Net.Sockets;

using SKBKontur.Cassandra.CassandraClient.Abstractions;
using SKBKontur.Cassandra.CassandraClient.Log;

using Thrift.Protocol;
using Thrift.Transport;

namespace SKBKontur.Cassandra.CassandraClient.Core
{
    public class ThriftConnection : IThriftConnection
    {
        protected ThriftConnection(int timeout, IPEndPoint ipEndPoint, string keyspaceName, ICassandraLogManager logManager)
        {
            IsAlive = true;
            logger = logManager.GetLogger(GetType());
            IpEndPoint = ipEndPoint;
            KeyspaceName = keyspaceName;
            string address = ipEndPoint.Address.ToString();
            int port = ipEndPoint.Port;
            TSocket tsocket = timeout == 0 ? new TSocket(address, port) : new TSocket(address, port, timeout);
            socket = tsocket.TcpClient.Client;
            socket.NoDelay = true;
            socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, 1);
            var transport = new TFramedTransport(tsocket);
            cassandraClient = new Apache.Cassandra.Cassandra.Client(new TBinaryProtocol(transport));
            lockObject = new object();
            OpenTransport();
        }

        public virtual void Dispose()
        {
            CloseTransport();
        }

        public void ExecuteCommand(ICommand command, ICassandraLogger logger)
        {
            lock(lockObject)
            {
                command.Execute(cassandraClient, logger);
            }
        }

        public bool IsAlive { get { return isAlive; } set { isAlive = value; } }

        public bool Ping()
        {
            try
            {
                lock(lockObject)
                {
                    cassandraClient.describe_version();
                }
                return true;
            }
            catch
            {
                return false;
            }
        }

        public override string ToString()
        {
            return string.Format("ThriftConnection[EndPoint='{0}' KeyspaceName='{1}']", IpEndPoint, KeyspaceName);
        }

        protected string KeyspaceName { get; set; }
        protected IPEndPoint IpEndPoint { get; set; }

        private void OpenTransport()
        {
            cassandraClient.InputProtocol.Transport.Open();
            if(!cassandraClient.InputProtocol.Transport.Equals(cassandraClient.OutputProtocol.Transport))
                cassandraClient.OutputProtocol.Transport.Open();
            if(!string.IsNullOrEmpty(KeyspaceName))
                cassandraClient.set_keyspace(KeyspaceName);
        }

        private void CloseTransport()
        {
            cassandraClient.InputProtocol.Transport.Close();
            if(!cassandraClient.InputProtocol.Transport.Equals(cassandraClient.OutputProtocol.Transport))
                cassandraClient.OutputProtocol.Transport.Close();
        }

        private volatile bool isAlive;
        private readonly Apache.Cassandra.Cassandra.Client cassandraClient;
        private readonly Socket socket;
        private ICassandraLogger logger;
        private readonly object lockObject;
    }
}