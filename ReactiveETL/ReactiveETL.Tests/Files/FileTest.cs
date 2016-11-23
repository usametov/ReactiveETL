using System;
using System.Text;
using System.Collections.Generic;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using FileHelpers;
using System.IO;

namespace ReactiveETL.Tests.Files
{
    /// <summary>
    /// Summary description for FileTest
    /// </summary>
    [TestClass]
    public class FileTest
    {
        [DelimitedRecord(";"), IgnoreFirst]
        private class FileReadPoco
        {
            public int Id { get; set; }
            public string Name { get; set; }
            public string Blabla { get; set; }
        }

        [TestMethod]
        public void FileReadTest()
        {
            using (var file = Helper.LoadFromRessourceFileToStreamReader("FileReadTest.txt"))
            {
                string content = file.ReadToEnd();
                file.BaseStream.Position = 0;
                var filecontent = Input
                    .ReadFile<FileReadPoco>(file)
                    .WriteFile<FileReadPoco>("resultfile.txt")
                    .Execute();
                Assert.IsTrue(File.Exists("resultfile.txt"));
                Assert.IsTrue(filecontent.Count == 2);
                File.Delete("resultfile.txt");
            }
        }
    }
}
