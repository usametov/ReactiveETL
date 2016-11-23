using System.Collections.Generic;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ReactiveETL.Tests.Joins
{
    [TestClass]
    public class JoinFixture : BaseJoinFixture
    {
        [TestMethod]
        public void InnerJoin()
        {
            var result = Input.From(left).InnerJoin(Input.From(right), "email", MergeRows).Execute();

            List<Row> items = (List<Row>)result.Data;

            Assert.AreEqual(1, items.Count);
            Assert.AreEqual(3, items[0]["person_id"]);
        }

        [TestMethod]
        public void RightJoin()
        {
            var result = Input.From(left).RightJoin(Input.From(right), "email", MergeRows).Execute();

            List<Row> items = (List<Row>)result.Data;

            Assert.AreEqual(2, items.Count);
            Assert.AreEqual(3, items[0]["person_id"]);
            Assert.IsNull(items[1]["name"]);
            Assert.AreEqual(5, items[1]["person_id"]);
        }

        [TestMethod]
        public void LeftJoin()
        {
            var result = Input.From(left).LeftJoin(Input.From(right), "email", MergeRows).Execute();
            List<Row> items = (List<Row>)result.Data;

            Assert.AreEqual(2, items.Count);
            Assert.AreEqual(3, items[0]["person_id"]);
            Assert.IsNull(items[1]["person_id"]);
            Assert.AreEqual("bar", items[1]["name"]);
        }

        [TestMethod]
        public void FullJoin()
        {
            var result = Input.From(left).FullJoin(Input.From(right), "email", MergeRows).Execute();
            List<Row> items = (List<Row>)result.Data;

            Assert.AreEqual(3, items.Count);

            Assert.AreEqual(3, items[0]["person_id"]);

            Assert.IsNull(items[1]["person_id"]);
            Assert.AreEqual("bar", items[1]["name"]);

            Assert.IsNull(items[2]["name"]);
            Assert.AreEqual(5, items[2]["person_id"]);
        }

        [TestMethod]
        public void CanJoinOnEnumerable()
        {
            var result = Input.From(left).Join(right, new RowJoinHelper("email").InnerJoinMatch, MergeRows).Execute();
            Assert.AreEqual(1, result.Count);
            Assert.AreEqual(3, ((List<Row>)result.Data)[0]["person_id"]);
        }

        [TestMethod]
        public void CanUseComplexJoinInProcesses()
        {
            var result =
                Input.From(left)
                .Transform(RowColumnToUpperCase)
                .RightJoin(Input.From(right).Transform(RowColumnToUpperCase), "email", MergeRows)
                .Execute();
            Assert.AreEqual(2, result.Count);
            Assert.AreEqual("FOO", ((List<Row>)result.Data)[0]["name"]);
            Assert.AreEqual("FOO@EXAMPLE.ORG", ((List<Row>)result.Data)[0]["email"]);
            Assert.AreEqual(5, ((List<Row>)result.Data)[1]["person_id"]);

        }

        private Row RowColumnToUpperCase(Row row)
        {
            foreach (string column in row.Columns)
            {
                string item = row[column] as string;
                if (item != null)
                    row[column] = item.ToUpper();
            }

            return row;
        }

        private Row MergeRows(Row leftRow, Row rightRow)
        {
            Row row = new Row();
            row.Copy(leftRow);
            if (rightRow != null)
                row["person_id"] = rightRow["id"];
            return row;
        }
    }
}
