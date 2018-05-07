namespace SkbKontur.Cassandra.Local
{
    public class LocalCassandraNode
    {
        public LocalCassandraNode(string templateDirectory, string deployDirectory)
        {
            TemplateDirectory = templateDirectory;
            DeployDirectory = deployDirectory;
            ClusterName = "local_cluster";
            LocalNodeName = "local_node";
            HeapSize = "1024M";
            const string localhostAddress = "127.0.0.1";
            RpcAddress = localhostAddress;
            ListenAddress = localhostAddress;
            SeedAddresses = new[] {localhostAddress};
            RpcPort = 9360;
            CqlPort = 9343;
            JmxPort = 7399;
            GossipPort = 7400;
        }

        public string TemplateDirectory { get; }
        public string DeployDirectory { get; }
        public string ClusterName { get; set; }
        public string LocalNodeName { get; set; }
        public string HeapSize { get; set; }
        public string RpcAddress { get; set; }
        public string ListenAddress { get; set; }
        public string[] SeedAddresses { get; set; }
        public int RpcPort { get; set; }
        public int CqlPort { get; set; }
        public int JmxPort { get; set; }
        public int GossipPort { get; set; }

        public override string ToString()
        {
            return $"{nameof(TemplateDirectory)}: {TemplateDirectory}, " +
                   $"{nameof(DeployDirectory)}: {DeployDirectory}, " +
                   $"{nameof(ClusterName)}: {ClusterName}, " +
                   $"{nameof(LocalNodeName)}: {LocalNodeName}, " +
                   $"{nameof(HeapSize)}: {HeapSize}, " +
                   $"{nameof(RpcAddress)}: {RpcAddress}, " +
                   $"{nameof(ListenAddress)}: {ListenAddress}, " +
                   $"{nameof(SeedAddresses)}: {string.Join(";", SeedAddresses)}, " +
                   $"{nameof(RpcPort)}: {RpcPort}, " +
                   $"{nameof(CqlPort)}: {CqlPort}, " +
                   $"{nameof(JmxPort)}: {JmxPort}, " +
                   $"{nameof(GossipPort)}: {GossipPort}";
        }
    }
}