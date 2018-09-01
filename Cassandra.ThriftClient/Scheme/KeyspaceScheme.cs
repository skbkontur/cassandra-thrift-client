namespace SKBKontur.Cassandra.CassandraClient.Scheme
{
    public class KeyspaceScheme
    {
        public string Name { get; set; }
        public KeyspaceConfiguration Configuration { get => configuration ?? new KeyspaceConfiguration(); set => configuration = value; }
        private KeyspaceConfiguration configuration = new KeyspaceConfiguration();
    }
}