using System.Linq;
using System.Text;

using Cassandra.Tests;

using NUnit.Framework;

using SKBKontur.Cassandra.CassandraClient.Abstractions;

namespace SKBKontur.Cassandra.FunctionalTests.Tests
{
    public class GetRowTest : CassandraFunctionalTestBase
    {
        [Test]
        public void TestGetFullRow()
        {
            var columns = new Column[14];
            for(int i = 0; i < columns.Length; i++)
            {
                string columnName = "columnName" + i;
                string columnValue = "columnValue" + i;
                columns[i] = ToColumn(columnName, columnValue, 100);
                columnFamilyConnection.AddColumn("row", columns[i]);
            }
            Column[] actualColumns = columnFamilyConnection.GetRow("row", 10).ToArray();
            actualColumns.AssertEqualsTo(columns.OrderBy(x => x.Name).ToArray());
        }

        [Test]
        public void TestGetAllRows()
        {
            var rows = new string[14];
            for(int i = 0; i < rows.Length; i++)
            {
                rows[i] = "row" + i;
                columnFamilyConnection.AddColumn(rows[i], new Column
                    {
                        Name = "columnName",
                        Value = Encoding.UTF8.GetBytes("columnValue"),
                        Timestamp = 100
                    });
            }
            string[] actualRows = columnFamilyConnection.GetKeys(10).ToArray();
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
                columnFamilyConnection.AddColumn("row", columns[i]);
            }
            Column[] actualColumns = columnFamilyConnection.GetColumns("row", null, 4);
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
                columnFamilyConnection.AddColumn("row", columns[i]);
            }
            Column[] actualColumns = columnFamilyConnection.GetColumns("row", null, 3);
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
            columnFamilyConnection.AddColumn(key, new Column
                {
                    Name = column.Name,
                    Value = column.Value,
                    Timestamp = timestamp
                });
            Column[] actualColumns = columnFamilyConnection.GetColumns(key, column.Name, 1);
            Assert.AreEqual(0, actualColumns.Length);
        }

        [Test]
        public void TestGetRowExclusiveFirstColumnWithReversed()
        {
            var columns = new Column[4];
            for (int i = 0; i < columns.Length; i++)
            {
                string columnName = "columnName" + i;
                string columnValue = "columnValue" + i;
                columns[i] = ToColumn(columnName, columnValue, 100);
                columnFamilyConnection.AddColumn("row", columns[i]);
            }
            Column[] actualColumns = columnFamilyConnection.GetColumns("row", "columnName3", 2, true);
            Assert.AreEqual(2, actualColumns.Length);
            for (int i = 0; i < 2; i++)
                actualColumns[i].AssertEqualsTo(columns[2 - i]);

            actualColumns = columnFamilyConnection.GetColumns("row", "columnName3", 999, true);
            Assert.AreEqual(3, actualColumns.Length);
            for (int i = 0; i < 3; i++)
                actualColumns[i].AssertEqualsTo(columns[2 - i]);
        }

        [Test]
        public void TestGetColumnsFormSlice()
        {
            var columns = new Column[10];
            for (int i = 0; i < columns.Length; i++)
            {
                string columnName = "columnName" + i;
                string columnValue = "columnValue" + i;
                columns[i] = ToColumn(columnName, columnValue, 100);
                columnFamilyConnection.AddColumn("row", columns[i]);
            }
            Column[] actualColumns = columnFamilyConnection.GetColumns("row", "columnName3", "columnName8", 3);
            Assert.AreEqual(3, actualColumns.Length);
            for (int i = 0; i < 3; i++)
                actualColumns[i].AssertEqualsTo(columns[i + 3]);

            actualColumns = columnFamilyConnection.GetColumns("row", "columnName3", "columnName8", 999);
            Assert.AreEqual(6, actualColumns.Length);
            for (int i = 0; i < 6; i++)
                actualColumns[i].AssertEqualsTo(columns[i + 3]);
        }

        [Test]
        public void TestGetColumnsFormSliceWithNoExistenceEndColumn()
        {
            var columns = new Column[6];
            for (int i = 0; i < columns.Length; i++)
            {
                string columnName = "columnName" + i;
                string columnValue = "columnValue" + i;
                columns[i] = ToColumn(columnName, columnValue, 100);
                columnFamilyConnection.AddColumn("row", columns[i]);
            }
            Column[] actualColumns = columnFamilyConnection.GetColumns("row", "columnName3", "columnName8", 50);
            Assert.AreEqual(3, actualColumns.Length);
            for (int i = 0; i < 3; i++)
                actualColumns[i].AssertEqualsTo(columns[i + 3]);
        }

        [Test]
        public void TestGetColumnsFormSliceReversed()
        {
            var columns = new Column[10];
            for (int i = 0; i < columns.Length; i++)
            {
                string columnName = "columnName" + i;
                string columnValue = "columnValue" + i;
                columns[i] = ToColumn(columnName, columnValue, 100);
                columnFamilyConnection.AddColumn("row", columns[i]);
            }
            Column[] actualColumns = columnFamilyConnection.GetColumns("row", "columnName8", "columnName3", 3, true);
            Assert.AreEqual(3, actualColumns.Length);
            for (int i = 0; i < 3; i++)
            {
                actualColumns[i].AssertEqualsTo(columns[8 - i]);
            }
        }

        [Test]
        public void TestGetColumnsByNames()
        {
            var columns = new Column[10];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = ToColumn("columnName" + i, "columnValue" + i, 100);
                columnFamilyConnection.AddColumn("row", columns[i]);
            }
            var actualColumns = columnFamilyConnection.GetColumns("row", new []{ "columnName5", "columnName3"});
            Assert.AreEqual(2, actualColumns.Length);
            
            actualColumns[0].AssertEqualsTo(columns[3]);
            actualColumns[1].AssertEqualsTo(columns[5]);
        }

        [Test]
        public void TestGetColumnsByNamesWhenOneColumnNameIsNotExists()
        {
            var columns = new Column[10];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = ToColumn("columnName" + i, "columnValue" + i, 100);
                columnFamilyConnection.AddColumn("row", columns[i]);
            }
            var actualColumns = columnFamilyConnection.GetColumns("row", new[] { "columnName5", "columnName11" });
            Assert.AreEqual(1, actualColumns.Length);

            actualColumns[0].AssertEqualsTo(columns[5]);
        }

        [Test]
        public void TestGetColumnsByNamesWhenNoElements()
        {
            var columns = new Column[10];
            for (int i = 0; i < columns.Length; i++)
            {
                columns[i] = ToColumn("columnName" + i, "columnValue" + i, 100);
                columnFamilyConnection.AddColumn("row", columns[i]);
            }
            var actualColumns = columnFamilyConnection.GetColumns("row", null);
            Assert.AreEqual(0, actualColumns.Length);
        }

        [Test]
        public void TestGetRowFirstColumnIsBig()
        {
            const string key = "a";
            columnFamilyConnection.AddColumn(key, new Column
                {
                    Name = "b",
                    Value = new byte[]{3}
                });
            CollectionAssert.IsEmpty(columnFamilyConnection.GetColumns(key, "c", 100));
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
                columnFamilyConnection.AddColumn(key, columns[i]);
            }
            Column[] actualColumns = columnFamilyConnection.GetColumns(key, columns[0].Name, 4);
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
                columnFamilyConnection.AddColumn(key, columns[i]);
            }
            Column[] actualColumns = columnFamilyConnection.GetColumns(key, "columnName", 4);
            Assert.AreEqual(4, actualColumns.Length);
            for(int i = 0; i < 4; i++)
                actualColumns[i].AssertEqualsTo(columns[i]);
        }

        [Test]
        public void TestGetRowEmpty()
        {
            CollectionAssert.IsEmpty(columnFamilyConnection.GetColumns("emptyRow", null, int.MaxValue));
        }

        [Test]
        public void TestGetRowFrom20()
        {
            var columns = new Column[1000];
            const string key = "key";
            for(int i = 0; i < columns.Length; i++)
            {
                string columnName = "columnName" + i.ToString("D3");
                string columnValue = "columnValue" + i;
                columns[i] = ToColumn(columnName, columnValue, 100);
            }
            var conn = cassandraCluster.RetrieveColumnFamilyConnection(KeyspaceName, Constants.ColumnFamilyName);
            conn.AddBatch(key, columns);
            var actualColumns = conn.GetRow(key, "columnName020", 20).ToArray();
            Assert.AreEqual(979, actualColumns.Length);
            for(int i = 0; i < actualColumns.Length; i++)
                actualColumns[i].AssertEqualsTo(columns[i + 21]);
        }
    }
}