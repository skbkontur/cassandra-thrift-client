using Apache.Cassandra;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions.Internal
{
    internal class SetMutation : IMutation
    {
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

        public Column Column { get; set; }
    }
}