using System;
using System.Collections.Generic;

using System.Text;

namespace CassandraClient.AquilesTrash.Encoders
{
    /// <summary>
    /// 
    /// </summary>
    public class UTF8EncoderHelper : IByteEncoderHelper<string>
    {
        private UTF8Encoding utf8ByteEncoder = new UTF8Encoding();
        #region IByteEncoderHelper<string> Members

        /// <summary>
        /// Transform a value into a Byte Array
        /// </summary>
        /// <param name="value">value to be transformed</param>
        /// <returns>a byte[]</returns>
        public byte[] ToByteArray(string value)
        {
            return utf8ByteEncoder.GetBytes(value);
        }
        /// <summary>
        /// get an instance with the value from the byte[]
        /// </summary>
        /// <param name="value">the byte[] with data</param>
        /// <returns>a new object</returns>
        public string FromByteArray(byte[] value)
        {
            return utf8ByteEncoder.GetString(value);
        }

        #endregion
    }
}
