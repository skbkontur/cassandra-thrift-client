using System;
using System.Collections;

using Apache.Cassandra;

using CassandraClient.AquilesTrash.Converter.Model;
using CassandraClient.AquilesTrash.Converter.Model.Impl;
using CassandraClient.AquilesTrash.Exceptions;
using CassandraClient.AquilesTrash.Model;

using Thrift.Protocol;

namespace CassandraClient.AquilesTrash.Converter
{
    /// <summary>
    /// Helper class to avoid creating Converters everytime needed.
    /// </summary>
    public static class ModelConverterHelper
    {
        static ModelConverterHelper()
        {
            object converter;
            converters = new Hashtable();

            // AquilesColumnConverter
            converter = new AquilesColumnConverter();
            converters.Add(typeof(AquilesColumn), converter);
            converters.Add(typeof(Column), converter);

            // AquilesColumnDefinitionConverter
            converter = new AquilesColumnDefinitionConverter();
            converters.Add(typeof(AquilesColumnDefinition), converter);
            converters.Add(typeof(ColumnDef), converter);

            // AquilesColumnFamilyConverter
            converter = new AquilesColumnFamilyConverter();
            converters.Add(typeof(AquilesColumnFamily), converter);
            converters.Add(typeof(CfDef), converter);

            // AquilesKeyspaceConverter
            converter = new AquilesKeyspaceConverter();
            converters.Add(typeof(AquilesKeyspace), converter);
            converters.Add(typeof(KsDef), converter);

            // AquilesMutationConverter
            converter = new AquilesMutationConverter();
            converters.Add(typeof(AquilesSetMutation), converter);
            converters.Add(typeof(AquilesDeletionMutation), converter);
            converters.Add(typeof(Mutation), converter);

            // AquilesSlicePredicateConverter
            converter = new AquilesSlicePredicateConverter();
            converters.Add(typeof(AquilesSlicePredicate), converter);
            converters.Add(typeof(SlicePredicate), converter);

            // AquilesSliceRangeConverter
            converter = new AquilesSliceRangeConverter();
            converters.Add(typeof(AquilesSliceRange), converter);
            converters.Add(typeof(SliceRange), converter);

            // AquilesSuperColumnConverter
            converter = new AquilesSuperColumnConverter();
            converters.Add(typeof(AquilesSuperColumn), converter);
            converters.Add(typeof(SuperColumn), converter);

            // AquilesIndexClauseConverter
            converter = new AquilesIndexClauseConverter();
            converters.Add(typeof(AquilesIndexClause), converter);
            converters.Add(typeof(IndexClause), converter);

            // AquilesIndexExpressionConverter
            converter = new AquilesIndexExpressionConverter();
            converters.Add(typeof(AquilesIndexExpression), converter);
            converters.Add(typeof(IndexExpression), converter);

            // AquilesKeyRangeConverter
            converter = new AquilesKeyRangeConverter();
            converters.Add(typeof(AquilesKeyRange), converter);
            converters.Add(typeof(KeyRange), converter);

            // AquilesTokenRangeConverter
            converter = new AquilesTokenRangeConverter();
            converters.Add(typeof(AquilesTokenRange), converter);
            converters.Add(typeof(TokenRange), converter);
        }

        /// <summary>
        /// convert from OBJECTA structure to OBJECTB structure
        /// </summary>
        /// <typeparam name="OBJECTA"></typeparam>
        /// <typeparam name="OBJECTB"></typeparam>
        /// <param name="from"></param>
        /// <returns></returns>
        public static OBJECTA Convert<OBJECTA, OBJECTB>(OBJECTB from)
            where OBJECTA : IAquilesObject
            where OBJECTB : TBase
        {
            OBJECTA to = default(OBJECTA);
            if(from != null)
            {
                IThriftConverter<OBJECTA, OBJECTB> converter = null;
                Type fromType = from.GetType();
                object temp = converters[fromType];
                if(temp != null)
                {
                    converter = (IThriftConverter<OBJECTA, OBJECTB>)temp;
                    to = converter.Transform(from);
                }
                else
                    throw new AquilesException(String.Format("No converter found for type '{0}'", fromType));
            }
            return to;
        }

        /// <summary>
        /// converts from OBJECTA structure to OBJECTB structure
        /// </summary>
        /// <typeparam name="OBJECTA"></typeparam>
        /// <typeparam name="OBJECTB"></typeparam>
        /// <param name="from"></param>
        /// <returns></returns>
        public static OBJECTB Convert<OBJECTA, OBJECTB>(OBJECTA from)
            where OBJECTA : IAquilesObject
            where OBJECTB : TBase
        {
            OBJECTB to = default(OBJECTB);
            if(from != null)
            {
                IThriftConverter<OBJECTA, OBJECTB> converter = null;
                Type fromType = from.GetType();
                object temp = converters[fromType];
                if(temp != null)
                {
                    converter = (IThriftConverter<OBJECTA, OBJECTB>)temp;
                    to = converter.Transform(from);
                }
                else
                    throw new AquilesException(String.Format("No converter found for type '{0}'", fromType));
            }
            return to;
        }

        private static readonly Hashtable converters;
    }
}