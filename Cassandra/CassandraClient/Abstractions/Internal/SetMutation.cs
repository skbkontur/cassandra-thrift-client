using Apache.Cassandra;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions.Internal
{
    internal class SetMutation<T> : IMutation where T : IColumn
    {
        public T Column { get; set; }

        public Mutation ToCassandraMutation()
        {
            return new Mutation
                {
                    Column_or_supercolumn = new ColumnOrSuperColumn
                        {
                            Column = Column.ToCassandraColumn()
                        }
                };
        }
    }
}