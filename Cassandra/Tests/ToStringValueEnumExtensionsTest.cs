using System;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace Cassandra.Tests
{
    public class ToStringValueEnumExtensionsTest : TestBase
    {
        [Test]
        public void TestCorrect()
        {
            Assert.AreEqual("AString", TestEnum1.A.ToStringValue());
            Assert.AreEqual("AString1", TestEnum2.A.ToStringValue());
            Assert.AreEqual("BString", TestEnum1.B.ToStringValue());
            Assert.AreEqual("BString1", TestEnum2.B.ToStringValue());

            Assert.AreEqual(TestEnum1.A, "AString".FromStringValue<TestEnum1>());
            Assert.AreEqual(TestEnum2.A, "AString1".FromStringValue<TestEnum2>());

            Assert.AreEqual(TestEnum1.B, "BString".FromStringValue<TestEnum1>());
            Assert.AreEqual(TestEnum2.B, "BString1".FromStringValue<TestEnum2>());

            Assert.AreEqual("CString", TestEnum1.C.ToStringValue());
            Assert.AreEqual("CString", TestEnum2.C.ToStringValue());
            Assert.AreEqual(TestEnum1.C, "CString".FromStringValue<TestEnum1>());
            Assert.AreEqual(TestEnum2.C, "CString".FromStringValue<TestEnum2>());
        }

        [Test, ExpectedException(ExpectedException = typeof(Exception), ExpectedMessage = "The enum value of type 'TestEnum2' not found for string value 'Unknown'")]
        public void TestUnknownString()
        {
            "Unknown".FromStringValue<TestEnum2>();
        }

        [Test]
        public void TestBadEnumGoodValue()
        {
            Assert.AreEqual("BString2", TestEnumBad.B.ToStringValue());
        }

        [Test, ExpectedException(ExpectedException = typeof(Exception), ExpectedMessage = "The string value not found for enum value 'A' of type 'TestEnumBad'")]
        public void TestBadEnumBadValue()
        {
            TestEnumBad.A.ToStringValue();
        }

        private enum TestEnum1
        {
            [StringValue("AString")]
            A,

            [StringValue("BString")]
            B,

            [StringValue("CString")]
            C
        }

        private enum TestEnum2
        {
            [StringValue("AString1")]
            A,

            [StringValue("BString1")]
            B,

            [StringValue("CString")]
            C
        }

        private enum TestEnumBad
        {
            A,

            [StringValue("BString2")]
            B
        }
    }
}