using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter.Model;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter.Model.Impl
{
    /// <summary>
    /// Converter for AquilesKeyRange
    /// </summary>
    public class AquilesTokenRangeConverter : IThriftConverter<AquilesTokenRange, TokenRange>
    {
        /// <summary>
        /// Transform AquilesTokenRange structure into TokenRange
        /// </summary>
        /// <param name="objectA"></param>
        /// <returns></returns>
        public TokenRange Transform(AquilesTokenRange objectA)
        {
            TokenRange tokenRange = new TokenRange();
            tokenRange.Endpoints = objectA.Endpoints;
            tokenRange.Start_token = objectA.StartToken;
            tokenRange.End_token = objectA.EndToken;

            return tokenRange;
        }

        /// <summary>
        /// Transform TokenRange structure into AquilesTokenRange
        /// </summary>
        /// <param name="objectB"></param>
        /// <returns></returns>
        public AquilesTokenRange Transform(TokenRange objectB)
        {
            AquilesTokenRange tokenRange = new AquilesTokenRange();
            tokenRange.Endpoints = objectB.Endpoints;
            tokenRange.StartToken = objectB.Start_token;
            tokenRange.EndToken = objectB.End_token;

            return tokenRange;
        }

        
    }
}
