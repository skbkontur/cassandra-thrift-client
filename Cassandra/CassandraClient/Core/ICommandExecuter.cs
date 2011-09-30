using CassandraClient.Abstractions;

namespace CassandraClient.Core
{
    public interface ICommandExecuter
    {
        void Execute(ICommand command);
    }
}