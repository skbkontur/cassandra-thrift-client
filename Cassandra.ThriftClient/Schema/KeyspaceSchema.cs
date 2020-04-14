namespace SkbKontur.Cassandra.ThriftClient.Schema
{
    public class KeyspaceSchema
    {
        public string Name { get; set; }
        public KeyspaceConfiguration Configuration { get => configuration ?? new KeyspaceConfiguration(); set => configuration = value; }
        private KeyspaceConfiguration configuration = new KeyspaceConfiguration();
    }
}