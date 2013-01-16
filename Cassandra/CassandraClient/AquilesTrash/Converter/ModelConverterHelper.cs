using System;
using System.Collections;

using Apache.Cassandra;

using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter.Model;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter.Model.Impl;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Exceptions;
using SKBKontur.Cassandra.CassandraClient.AquilesTrash.Model;

using Thrift.Protocol;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Converter
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
            
            // AquilesSuperColumnConverter
            converter = new AquilesSuperColumnConverter();
            converters.Add(typeof(AquilesSuperColumn), converter);
            converters.Add(typeof(SuperColumn), converter);

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