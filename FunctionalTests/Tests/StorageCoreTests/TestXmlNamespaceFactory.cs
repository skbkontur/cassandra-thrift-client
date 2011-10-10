using GroboSerializer.XmlNamespaces;

namespace SKBKontur.Cassandra.FunctionalTests.StorageCoreTests
{
    public class TestXmlNamespaceFactory : IXmlNamespaceFactory
    {
        #region IXmlNamespaceFactory Members

        public XmlNamespace GetNamespace(string namespacePrefix)
        {
            return XmlNamespace.Default;
        }

        #endregion
    }
}