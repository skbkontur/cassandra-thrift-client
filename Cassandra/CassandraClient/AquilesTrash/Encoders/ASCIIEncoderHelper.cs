using System.Text;

namespace CassandraClient.AquilesTrash.Encoders
{
    /// <summary>
    /// Encoder Helper for ASCII
    /// </summary>
    public class ASCIIEncoderHelper : IByteEncoderHelper<string>
    {
        private ASCIIEncoding byteEncoder = new ASCIIEncoding();

        /// <summary>
        /// Transform a value into a Byte Array
        /// </summary>
        /// <param name="value">value to be transformed</param>
        /// <returns>a byte[]</returns>
        public byte[] ToByteArray(string value)
        {
            return byteEncoder.GetBytes(value);
        }
        /// <summary>
        /// get an instance with the value from the byte[]
        /// </summary>
        /// <param name="value">the byte[] with data</param>
        /// <returns>a new object</returns>
        public string FromByteArray(byte[] value)
        {
            return byteEncoder.GetString(value);
        }

        
    }
}
