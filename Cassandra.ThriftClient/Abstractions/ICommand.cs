using Vostok.Logging.Abstractions;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions
{
    internal interface ICommand
    {
        void Execute(Apache.Cassandra.Cassandra.Client client, ILog logger);
        string Name { get; }
        CommandContext CommandContext { get; }
    }
}