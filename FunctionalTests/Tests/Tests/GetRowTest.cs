using Cassandra.Tests;
using CassandraClient.Abstractions;
using NUnit.Framework;
using System.Linq;

namespace Tests.Tests
{
    public class GetRowTest : CassandraFunctionalTestWithRemoveKeyspacesBase
    {
        [Test]
        public void TestGetFullRow()
        {
            var columns = new Column[14];
            for (int i = 0; i < columns.Length; i++)
            {
                string columnName = "columnName" + i;
                string columnValue = "columnValue" + i;
                columns[i] = ToColumn(columnName, columnValue, 100);
                cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, "row", columnName, columnValue, 100);
            }
            Column[] actualColumns = cassandraClient.GetRow(Constants.KeyspaceName, Constants.ColumnFamilyName, "row").ToArray();
            actualColumns.AssertEqualsTo(columns.OrderBy(x => x.Name).ToArray());
        }

        [Test]
        public void TestGetAllRows()
        {
            var rows = new string[14];
            for (int i = 0; i < rows.Length; i++)
            {
                rows[i] = "row" + i;
                cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, rows[i], "columnName", "columnValue", 100);
            }
            string[] actualRows = cassandraClient.GetKeys(Constants.KeyspaceName, Constants.ColumnFamilyName).ToArray();
            actualRows.OrderBy(x => x).ToArray().AssertEqualsTo(rows.OrderBy(x => x).ToArray());
        }

        [Test]
        public void TestGetRow()
        {
            var columns = new Column[4];
            for(int i = 0; i < columns.Length; i++)
            {
                string columnName = "columnName" + i;
                string columnValue = "columnValue" + i;
                columns[i] = ToColumn(columnName, columnValue, 100);
                cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, "row", columnName, columnValue, 100);
            }
            Column[] actualColumns = cassandraClient.GetRow(Constants.KeyspaceName, Constants.ColumnFamilyName, "row", 4);
            actualColumns.AssertEqualsTo(columns);
        }

        [Test]
        public void TestGetRowSmallCount()
        {
            var columns = new Column[4];
            for(int i = 0; i < columns.Length; i++)
            {
                string columnName = "columnName" + i;
                string columnValue = "columnValue" + i;
                columns[i] = ToColumn(columnName, columnValue, 100);
                cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, "row", columnName, columnValue, 100);
            }
            Column[] actualColumns = cassandraClient.GetRow(Constants.KeyspaceName, Constants.ColumnFamilyName, "row", 3);
            Assert.AreEqual(3, actualColumns.Length);
            for(int i = 0; i < 3; i++)
                actualColumns[i].AssertEqualsTo(columns[i]);
        }

        [Test]
        public void TestGetRowExclusiveFirstColumn()
        {
            const string columnName = "columnName";
            const string columnValue = "columnValue";
            const int timestamp = 5738;
            Column column = ToColumn(columnName, columnValue, timestamp);
            const string key = "row";
            cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, key, column.Name, column.Value, timestamp);
            Column[] actualColumns = cassandraClient.GetRow(Constants.KeyspaceName, Constants.ColumnFamilyName, key, 1, column.Name);
            Assert.AreEqual(0, actualColumns.Length);
        }

        [Test]
        public void TestGetRowFirstColumnIsBig()
        {
            const string key = "a";
            cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, key, "b", new byte[] {3});
            CollectionAssert.IsEmpty(cassandraClient.GetRow(Constants.KeyspaceName, Constants.ColumnFamilyName, key, 100, "c"));
        }

        [Test]
        public void TestGetRowReturnLessThanQuery()
        {
            var columns = new Column[4];
            const string key = "key";
            for(int i = 0; i < columns.Length; i++)
            {
                string columnName = "columnName" + i;
                string columnValue = "columnValue" + i;
                columns[i] = ToColumn(columnName, columnValue, 100);
                cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, key, columns[i].Name, columns[i].Value, 100);
            }
            Column[] actualColumns = cassandraClient.GetRow(Constants.KeyspaceName, Constants.ColumnFamilyName, key, 4, columns[0].Name);
            Assert.AreEqual(3, actualColumns.Length);
            for(int i = 0; i < 3; i++)
                actualColumns[i].AssertEqualsTo(columns[i + 1]);
        }

        [Test]
        public void TestGetFirstColumns()
        {
            var columns = new Column[5];
            const string key = "key";
            for(int i = 0; i < columns.Length; i++)
            {
                string columnName = "columnName" + i;
                string columnValue = "columnValue" + i;
                columns[i] = ToColumn(columnName, columnValue, 100);
                cassandraClient.Add(Constants.KeyspaceName, Constants.ColumnFamilyName, key, columns[i].Name, columns[i].Value, 100);
            }
            Column[] actualColumns = cassandraClient.GetRow(Constants.KeyspaceName, Constants.ColumnFamilyName, key, 4, "columnName");
            Assert.AreEqual(4, actualColumns.Length);
            for(int i = 0; i < 4; i++)
                actualColumns[i].AssertEqualsTo(columns[i]);
        }

        [Test]
        public void TestGetRowEmpty()
        {
            CollectionAssert.IsEmpty(cassandraClient.GetRow(Constants.KeyspaceName, Constants.ColumnFamilyName, "emptyRow", int.MaxValue));
        }
    }
}