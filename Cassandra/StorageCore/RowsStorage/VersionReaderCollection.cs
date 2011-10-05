using GroboSerializer;

namespace StorageCore.RowsStorage
{
    public class VersionReaderCollection : IVersionReaderCollection
    {
        public VersionReaderCollection(ISerializer serializer)
        {
            version1Reader = new Version1Reader(serializer);
            version2Reader = new Version2Reader(serializer);
        }

        public IVersionReader GetVersionReader(string version)
        {
            if(version == FormatVersions.version1)
                return version1Reader;
            if(version == FormatVersions.version2)
                return version2Reader;
            throw new UnknownFormatVersionException(version);
        }

        private readonly Version1Reader version1Reader;
        private readonly Version2Reader version2Reader;
    }
}