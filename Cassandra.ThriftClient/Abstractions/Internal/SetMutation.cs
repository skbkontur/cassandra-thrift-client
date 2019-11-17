using Apache.Cassandra;

namespace SkbKontur.Cassandra.ThriftClient.Abstractions.Internal
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

        public RawColumn Column { get; set; }
    }
}