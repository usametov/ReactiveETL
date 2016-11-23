using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace ReactiveETL.Tests.Files
{
    [TestClass]
    public class UsingDALFixture
    {
        private const string expected =
            @"Id	Name	Email
1	ayende	ayende@example.org
2	foo	foo@example.org
3	bar	bar@example.org
4	brak	brak@example.org
5	snar	snar@example.org
";
        [TestMethod]
        public void CanWriteToFileFromDAL()
        {
            Input
                .From(MySimpleDal.GetUsers())
                .WriteFile<UserRecord>(
                    "users.txt", 
                    ff => ff.HeaderText = "Id\tName\tEmail")
                .Start();
            string actual = File.ReadAllText("users.txt");
            Assert.AreEqual(expected.Replace("\r\n", "\n").Replace("\n", Environment.NewLine), actual);
        }

        [TestMethod]
        public void CanReadFromFileToDAL()
        {
            MySimpleDal.Users = new List<User>();
            File.WriteAllText("users.txt", expected);

            Input
                .ReadFile<UserRecord>("users.txt")
                .Apply(row => MySimpleDal.Save(row.ToObject<User>()))
                .Start();

            Assert.AreEqual(5, MySimpleDal.Users.Count);
        }
    }
}
