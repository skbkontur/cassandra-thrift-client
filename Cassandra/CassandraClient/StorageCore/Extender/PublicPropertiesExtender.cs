namespace CassandraClient.StorageCore.Extender
{
    internal class PublicPropertiesExtender
    {
        public PublicPropertiesExtender()
        {
            impl = new ExtenderImpl(ExtenderImpl.ScanProperties.AllPublic);
        }

        public void Extend<T>(T source)
        {
            impl.Extend(typeof(T), source);
        }

        private readonly ExtenderImpl impl;
    }
}