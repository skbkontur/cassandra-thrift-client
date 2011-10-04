using CassandraClient.AquilesTrash.Model;

using Thrift.Protocol;

namespace CassandraClient.AquilesTrash.Converter.Model
{
    /// <summary>
    /// Interface for converter between objects structure
    /// </summary>
    /// <typeparam name="OBJECTA"></typeparam>
    /// <typeparam name="OBJECTB"></typeparam>
    public interface IThriftConverter<OBJECTA, OBJECTB>
        where OBJECTA : IAquilesObject
        where OBJECTB : TBase
    {
        /// <summary>
        /// Transform OBJECTA structure into OBJECTB
        /// </summary>
        /// <param name="objectA"></param>
        /// <returns></returns>
        OBJECTB Transform(OBJECTA objectA);
        /// <summary>
        /// Transform OBJECTB structure into OBJECTA
        /// </summary>
        /// <param name="objectB"></param>
        /// <returns></returns>
        OBJECTA Transform(OBJECTB objectB);
    }
}
