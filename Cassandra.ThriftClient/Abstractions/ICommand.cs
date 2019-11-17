using Vostok.Logging.Abstractions;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions
{
    internal interface ICommand
    {
        void Execute(Apache.Cassandra.Cassandra.Client client, ILog logger);
        string Name { get; }
        CommandContext CommandContext { get; }
    }
}