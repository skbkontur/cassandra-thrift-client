using System.Net;

using Apache.Cassandra;

using CassandraClient.Abstractions;

using Thrift.Protocol;
using Thrift.Transport;

namespace CassandraClient.Core
{
    public class ThriftConnection : IThriftConnection
    {
        protected ThriftConnection(int timeout, IPEndPoint ipEndPoint, string keyspaceName)
        {
            IpEndPoint = ipEndPoint;
            KeyspaceName = keyspaceName;
            string address = ipEndPoint.Address.ToString();
            int port = ipEndPoint.Port;
            TSocket socket = timeout == 0 ? new TSocket(address, port) : new TSocket(address, port, timeout);
            socket.TcpClient.Client.NoDelay = true;
            var transport = new TFramedTransport(socket);
            cassandraClient = new Cassandra.Client(new TBinaryProtocol(transport));
            OpenTransport();
        }

        public virtual void Dispose()
        {
            CloseTransport();
        }

        public void ExecuteCommand(ICommand command)
        {
            command.Execute(cassandraClient);
        }

        public bool IsAlive()
        {
            try
            {
                return (cassandraClient.InputProtocol.Transport.IsOpen && cassandraClient.OutputProtocol.Transport.IsOpen);
            }
            catch
            {
                return false;
            }
        }

        public string KeyspaceName { get; private set; }
        public IPEndPoint IpEndPoint { get; private set; }

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

        private readonly Cassandra.Client cassandraClient;
    }
}