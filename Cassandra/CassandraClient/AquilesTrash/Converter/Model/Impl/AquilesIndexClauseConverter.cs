using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;
using CassandraClient.AquilesTrash.Model;

using CassandraClient.AquilesTrash.Converter;
using CassandraClient.AquilesTrash.Converter.Model;

namespace CassandraClient.AquilesTrash.Converter.Model.Impl
{
    /// <summary>
    /// Converter for AquilesIndexClause
    /// </summary>
    public class AquilesIndexClauseConverter : IThriftConverter<AquilesIndexClause, IndexClause>
    {
        #region IThriftConverter<AquilesIndexClause,IndexClause> Members
        /// <summary>
        /// Transform AquilesIndexClause structure into IndexClause
        /// </summary>
        /// <param name="objectA"></param>
        /// <returns></returns>
        public IndexClause Transform(AquilesIndexClause objectA)
        {
            IndexClause indexClause = new IndexClause();
            List<IndexExpression> expressions = new List<IndexExpression>(objectA.Expressions.Count);
            foreach(AquilesIndexExpression indexExpression in objectA.Expressions)
            {
                expressions.Add(ModelConverterHelper.Convert<AquilesIndexExpression, IndexExpression>(indexExpression));
            }
            indexClause.Expressions = expressions;
            indexClause.Start_key = objectA.StartKey;
            if (objectA.Count.HasValue)
            {
                indexClause.Count = objectA.Count.Value;
            }
            return indexClause;
        }

        /// <summary>
        /// Transform IndexClause structure into AquilesIndexClause
        /// </summary>
        /// <param name="objectB"></param>
        /// <returns></returns>
        public AquilesIndexClause Transform(IndexClause objectB)
        {
            AquilesIndexClause indexClause = new AquilesIndexClause();
            List<AquilesIndexExpression> expressions = new List<AquilesIndexExpression>(objectB.Expressions.Count);
            foreach(IndexExpression indexExpression in objectB.Expressions) {
                expressions.Add(ModelConverterHelper.Convert<AquilesIndexExpression, IndexExpression>(indexExpression));
            }
            indexClause.Expressions = expressions;
            indexClause.StartKey = objectB.Start_key;
            indexClause.Count = objectB.Count;

            return indexClause;
        }

        #endregion
    }
}
