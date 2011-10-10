using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SKBKontur.Cassandra.CassandraClient.AquilesTrash.Encoders
{
    /// <summary>
    /// Encoder Helper for UUIDs (Guids that are compatible with UUID java version)
    /// <remarks>see http://coderjournal.com/2010/05/timeuuid-only-makes-sense-with-version-1-uuids/</remarks>
    /// </summary>
    public class UUIDEncoderHelper : IByteEncoderHelper<Guid>
    {
        /// <summary>
        /// Transform a value into a Byte Array
        /// </summary>
        /// <param name="value">value to be transformed</param>
        /// <returns>a byte[]</returns>
        public byte[] ToByteArray(Guid value)
        {
            byte[] oldValue = ByteEncoderHelper.GuidEncoder.ToByteArray(value);
            byte[] newValue = new byte[16];
            Array.Copy(ReverseLowFieldTimestamp(oldValue), 0, newValue, 0, 4);
            Array.Copy(ReverseMiddleFieldTimestamp(oldValue), 0, newValue, 4, 2);
            Array.Copy(ReverseHighFieldTimestamp(oldValue), 0, newValue, 6, 2);
            Array.Copy(oldValue, 8, newValue, 8, 8);
            return newValue;
        }

        /// <summary>
        /// get an instance with the value from the byte[]
        /// </summary>
        /// <param name="value">the byte[] with data</param>
        /// <returns>a new object</returns>
        public Guid FromByteArray(byte[] value)
        {
            byte[] newValue = new byte[16];
            Array.Copy(ReverseLowFieldTimestamp(value), 0, newValue, 0, 4);
            Array.Copy(ReverseMiddleFieldTimestamp(value), 0, newValue, 4, 2);
            Array.Copy(ReverseHighFieldTimestamp(value), 0, newValue, 6, 2);
            Array.Copy(value, 8, newValue, 8, 8);
            return new Guid(newValue);
        }

        

        private byte[] ReverseLowFieldTimestamp(byte[] guid)
        {
            return guid.Skip(0).Take(4).Reverse().ToArray();
        }

        private byte[] ReverseMiddleFieldTimestamp(byte[] guid)
        {
            return guid.Skip(4).Take(2).Reverse().ToArray();
        }

        private byte[] ReverseHighFieldTimestamp(byte[] guid)
        {
            return guid.Skip(6).Take(2).Reverse().ToArray();
        }
    }
}
