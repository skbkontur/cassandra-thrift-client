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

        public void ExecuteCommand(ICommand command, ICassandraLogger logger)
        {
            DoActionWithCloseTransportIfCrash(() => command.Execute(cassandraClient, logger));
        }

        public bool IsAlive { get { return isAlive && CassandraTransportIsOpen(); } private set { isAlive = value; } }

        public void Check()
        {
            DoActionWithCloseTransportIfCrash(() => cassandraClient.describe_version());
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
            lock(lockObject)
            {
                try
                {
                    IsAlive = false;
                    cassandraClient.InputProtocol.Transport.Close();
                    if(!cassandraClient.InputProtocol.Transport.Equals(cassandraClient.OutputProtocol.Transport))
                        cassandraClient.OutputProtocol.Transport.Close();
                }
                catch(Exception e)
                {
                    logger.Debug(e, "Exception while close transport");
                }
            }
        }

        private void DoActionWithCloseTransportIfCrash(Action action)
        {
            lock(lockObject)
            {
                try
                {
                    action();
                }
                catch(Exception e)
                {
                    CloseTransport();
                    logger.Debug(e, "Some error during action in cassandra");
                }
            }
        }

        private volatile bool isAlive;
        private readonly Apache.Cassandra.Cassandra.Client cassandraClient;
        private readonly Socket socket;
        private readonly ICassandraLogger logger;
        private readonly object lockObject;
    }
}