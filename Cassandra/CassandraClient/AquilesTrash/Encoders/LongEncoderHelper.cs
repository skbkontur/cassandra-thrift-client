using System;
using System.Collections.Generic;

using System.Text;

namespace CassandraClient.AquilesTrash.Encoders
{
    /// <summary>
    /// Encoder Helper for Long
    /// </summary>
    public class LongEncoderHelper : IByteEncoderHelper<long>
    {
        //TODO Ver de hacer un long encoder de endian y otro para big-endian
        /// <summary>
        /// Transform a value into a Byte Array
        /// </summary>
        /// <param name="value">value to be transformed</param>
        /// <returns>a byte[]</returns>
        public byte[] ToByteArray(long value)
        {
            return BitConverter.GetBytes(value);
        }
        /// <summary>
        /// get an instance with the value from the byte[]
        /// </summary>
        /// <param name="value">the byte[] with data</param>
        /// <returns>a new object</returns>
        public long FromByteArray(byte[] value)
        {
            return BitConverter.ToInt64(value, 0);
        }

        
    }
}
