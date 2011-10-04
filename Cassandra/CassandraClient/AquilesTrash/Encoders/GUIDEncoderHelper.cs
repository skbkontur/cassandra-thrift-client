using System;
using System.Collections.Generic;

using System.Text;

namespace CassandraClient.AquilesTrash.Encoders
{
    /// <summary>
    /// Encoder Helper for Guids
    /// </summary>
    public class GUIDEncoderHelper : IByteEncoderHelper<Guid>
    {
        #region IByteEncoderHelper<Guid> Members

        /// <summary>
        /// Transform a value into a Byte Array
        /// </summary>
        /// <param name="value">value to be transformed</param>
        /// <returns>a byte[]</returns>
        public byte[] ToByteArray(Guid value)
        {
            return value.ToByteArray();
        }

        /// <summary>
        /// get an instance with the value from the byte[]
        /// </summary>
        /// <param name="value">the byte[] with data</param>
        /// <returns>a new object</returns>
        public Guid FromByteArray(byte[] value)
        {
            return new Guid(value);
        }

        #endregion
    }
}
