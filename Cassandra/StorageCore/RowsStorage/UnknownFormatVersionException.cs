using System;

namespace StorageCore.RowsStorage
{
    public class UnknownFormatVersionException : Exception
    {
        public UnknownFormatVersionException(string version)
            : base(string.Format("Unknown version: {0}", version))
        {
        }
    }
}