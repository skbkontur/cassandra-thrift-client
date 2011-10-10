using System;
using System.Collections.Generic;

using System.Text;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Encoders
{
    /// <summary>
    /// Interface for Byte Encoder
    /// </summary>
    /// <typeparam name="TYPE">class type applied</typeparam>
    public interface IByteEncoderHelper<TYPE>
    {
        /// <summary>
        /// Transform a value into a Byte Array
        /// </summary>
        /// <param name="value">value to be transformed</param>
        /// <returns>a byte[]</returns>
        Byte[] ToByteArray(TYPE value);
        /// <summary>
        /// get an instance with the value from the byte[]
        /// </summary>
        /// <param name="value">the byte[] with data</param>
        /// <returns>a new object</returns>
        TYPE FromByteArray(Byte[] value);
    }
}
