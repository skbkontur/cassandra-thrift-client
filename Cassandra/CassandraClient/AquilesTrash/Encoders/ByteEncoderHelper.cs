using System;
using System.Collections.Generic;

using System.Text;

namespace CassandraClient.AquilesTrash.Encoders
{
    /// <summary>
    /// Encoder Helper 
    /// </summary>
    public static class ByteEncoderHelper
    {
        /// <summary>
        /// Encoder Helper for Long
        /// </summary>
        public static readonly IByteEncoderHelper<long> LongEncoder;
        /// <summary>
        /// Encoder Helper for ASCII
        /// </summary>
        public static readonly IByteEncoderHelper<string> ASCIIEncoder;
        /// <summary>
        /// Encoder Helper for UTF8
        /// </summary>
        public static readonly IByteEncoderHelper<string> UTF8Encoder;
        /// <summary>
        /// Encoder Helper for GUID
        /// </summary>
        public static readonly IByteEncoderHelper<Guid> GuidEncoder;

        /// <summary>
        /// Encoder Helper for UUID
        /// </summary>
        public static readonly IByteEncoderHelper<Guid> UUIDEnconder;

        static ByteEncoderHelper()
        {
            LongEncoder = new LongEncoderHelper();
            ASCIIEncoder = new ASCIIEncoderHelper();
            UTF8Encoder = new UTF8EncoderHelper();
            GuidEncoder = new GUIDEncoderHelper();
            UUIDEnconder = new UUIDEncoderHelper();
        }

    }
}

