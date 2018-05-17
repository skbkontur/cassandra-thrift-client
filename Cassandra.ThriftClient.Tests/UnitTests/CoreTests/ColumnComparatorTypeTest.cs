using System;
using System.Linq;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace Cassandra.Tests.CoreTests
{
    public class ColumnComparatorTypeTest
    {
        [Test]
        public void TestInvalidCreateColumnComparatorType()
        {
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType(""));
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType((string)null));
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType("some-string"));
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType(DataType.CompositeType.ToStringValue()));
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType(string.Format("{0}({1})", DataType.CompositeType.ToStringValue(), DataType.Int32Type.ToStringValue())));
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType(string.Format("{0}({1},{2})", DataType.CompositeType.ToStringValue(), DataType.CompositeType.ToStringValue(), DataType.CompositeType.ToStringValue())));
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType(string.Format("{0}({1},{2})", DataType.CompositeType.ToStringValue(), DataType.CompositeType.ToStringValue(), DataType.Int32Type.ToStringValue())));
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType(string.Format("{0},{1},{2}", DataType.CompositeType.ToStringValue(), DataType.UTF8Type.ToStringValue(), DataType.Int32Type.ToStringValue())));
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType(string.Format("{0},{1}", DataType.UTF8Type.ToStringValue(), DataType.Int32Type.ToStringValue())));
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType((DataType[])null));
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType(new DataType[0]));
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType(new[] {DataType.CompositeType}));
            Assert.Throws<InvalidOperationException>(() => new ColumnComparatorType(new[] {DataType.DateType, DataType.CompositeType}));
        }

        [Test]
        public void TestCreateFromStringSingleDataType()
        {
            var input = DataType.UTF8Type.ToStringValue();
            var comparatorType = new ColumnComparatorType(input);
            Assert.That(comparatorType.IsComposite, Is.False);
            Assert.That(comparatorType.Types, Is.EqualTo(new[] {DataType.UTF8Type}));
            Assert.That(comparatorType.ToString(), Is.EqualTo(input));
        }

        [Test]
        public void TestCreateFromStringCompositeDataType()
        {
            var input = string.Format("{0}({1},{2},{3})", DataType.CompositeType.ToStringValue(), DataType.UTF8Type.ToStringValue(), DataType.Int32Type.ToStringValue(), DataType.FloatType.ToStringValue());
            var comparatorType = new ColumnComparatorType(input);
            Assert.That(comparatorType.IsComposite, Is.True);
            Assert.That(comparatorType.Types, Is.EqualTo(new[] {DataType.UTF8Type, DataType.Int32Type, DataType.FloatType}));
            Assert.That(comparatorType.ToString(), Is.EqualTo(input));
        }

        [Test]
        public void TestCreateFromSubType()
        {
            var comparatorType = new ColumnComparatorType(new[] {DataType.BytesType});
            Assert.That(comparatorType.IsComposite, Is.False);
            Assert.That(comparatorType.Types, Is.EqualTo(new[] {DataType.BytesType}));
            Assert.That(comparatorType.ToString(), Is.EqualTo(DataType.BytesType.ToStringValue()));
        }

        [Test]
        public void TestCreateFromSubTypes()
        {
            var subTypes = new[] {DataType.BytesType, DataType.AsciiType, DataType.DateType};
            var comparatorType = new ColumnComparatorType(subTypes);
            Assert.That(comparatorType.IsComposite, Is.True);
            Assert.That(comparatorType.Types, Is.EqualTo(subTypes));
            Assert.That(comparatorType.ToString(), Is.EqualTo(string.Format("{0}({1})", DataType.CompositeType.ToStringValue(), string.Join(",", subTypes.Select(x => x.ToStringValue())))));
        }
    }
}