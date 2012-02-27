using System;
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

        public void ExecuteCommand(ICommand command)
        {
            try
            {
                lock(lockObject)
                {
                    command.Execute(cassandraClient, logger);
                }
            }
            catch(Exception e)
            {
                CloseTransport();
                logger.Warn(e, "Exception during execute command {0}", command.GetCommandType().Name);
                throw;
            }
        }

        public bool IsAlive { get { return isAlive && CassandraTransportIsOpen(); } private set { isAlive = value; } }

        public void Check()
        {
            try
            {
                lock(lockObject)
                {
                    cassandraClient.describe_version();
                }
            }
            catch(Exception e)
            {
                CloseTransport();
                logger.Debug(e, "Exception in method Check. Close transport.");
            }
        }

        public override string ToString()
        {
            return string.Format("ThriftConnection[EndPoint='{0}' KeyspaceName='{1}']", IpEndPoint, KeyspaceName);
        }

        protected string KeyspaceName { get; set; }
        protected IPEndPoint IpEndPoint { get; set; }

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
                if(!string.IsNullOrEmpty(KeyspaceName))
                    cassandraClient.set_keyspace(KeyspaceName);
            }
        }

        private void CloseTransport()
        {
            try
            {
                lock(lockObject)
                {
                    IsAlive = false;
                    cassandraClient.InputProtocol.Transport.Close();
                    if(!cassandraClient.InputProtocol.Transport.Equals(cassandraClient.OutputProtocol.Transport))
                        cassandraClient.OutputProtocol.Transport.Close();
                }
            }
            catch(Exception e)
            {
                logger.Debug(e, "Exception while close transport");
            }
        }

        private volatile bool isAlive;
        private readonly Apache.Cassandra.Cassandra.Client cassandraClient;
        private readonly Socket socket;
        private readonly ICassandraLogger logger;
        private readonly object lockObject;
    }
}