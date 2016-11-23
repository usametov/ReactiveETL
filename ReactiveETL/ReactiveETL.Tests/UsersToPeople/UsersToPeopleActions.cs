using System.Data;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ReactiveETL.Infrastructure;

namespace ReactiveETL.Tests
{
    class UsersToPeopleActions
    {
        public static string SelectAllUsers
        {
            get
            {
                return "SELECT * FROM Users";
            }
        }

        public static Row SplitUserName(Row row)
        {
            string name = (string)row["name"];
            row["FirstName"] = name.Split()[0];
            row["LastName"] = name.Split()[1];
            return row;
        }

        public static void WritePeople(IDbCommand cmd, Row row)
        {
            cmd.CommandText =
                @"INSERT INTO People (UserId, FirstName, LastName, Email) VALUES (@UserId, @FirstName, @LastName, @Email)";
            cmd.AddParameter("UserId", row["Id"]);
            cmd.AddParameter("FirstName", row["FirstName"]);
            cmd.AddParameter("LastName", row["LastName"]);
            cmd.AddParameter("Email", row["Email"]);
        }        

        public static void VerifyResult(EtlFullResult result)
        {
            Assert.IsTrue(result.Completed);
            Assert.AreEqual(4, result.Count);
            Assert.AreEqual(0, result.CountExceptions);
            
            System.Collections.Generic.List<string[]> names = Use.Transaction<System.Collections.Generic.List<string[]>>("test", delegate(IDbCommand cmd)
            {
                System.Collections.Generic.List<string[]> tuples = new System.Collections.Generic.List<string[]>();
                cmd.CommandText = "SELECT firstname, lastname from people order by userid";
                using (IDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        tuples.Add(new string[] { reader.GetString(0), reader.GetString(1) });
                    }
                }
                return tuples;
            });
            BaseUserToPeopleTest.AssertNames(names);
        } 
    }
}
