using System.Collections.Generic;
using System.Linq;

namespace SKBKontur.Cassandra.CassandraClient.Abstractions.Internal
{
    internal class IndexClause
    {
        public List<RawIndexExpression> Expressions { get; set; }
        public byte[] StartKey { get; set; }
        public int? Count { get; set; }
    }

    internal static class IndexClauseExtensions
    {
        public static Apache.Cassandra.IndexClause ToCassandraIndexClause(this IndexClause indexClause)
        {
            if(indexClause == null)
                return null;
            var result = new Apache.Cassandra.IndexClause();
            var expressions = indexClause.Expressions ?? new List<RawIndexExpression>();
            result.Expressions = expressions.Select(expression => expression.ToCassandraIndexExpression()).ToList();
            if(indexClause.Count.HasValue)
                result.Count = indexClause.Count.Value;
            result.Start_key = indexClause.StartKey;
            return result;
        }
    }
}