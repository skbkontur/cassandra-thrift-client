using Apache.Cassandra;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions.Internal
{
    internal class SetMutation : IMutation
    {
        public RawColumn Column { get; set; }

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