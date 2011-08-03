using GroboSerializer.XmlNamespaces;

namespace Tests.StorageCoreTests
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