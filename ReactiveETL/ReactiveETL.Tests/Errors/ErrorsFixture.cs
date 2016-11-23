using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using ReactiveETL.Exceptions;

namespace ReactiveETL.Tests
{
    [TestClass]
    public class ErrorsFixture
    {        
        public IEnumerable<User> ListUsers(int numusers)
        {
            for (int i=0 ; i < numusers; i++)
            {
                yield return new User() {Id = i, Email = "1@rhino.com", Name = "User" + i};
            }
        }

        [TestMethod]
        public void WillReportErrorsWhenThrown()
        {
            int maxElements = 1000;
            int throwAfter = 15;
            int rowCount = 0;

            try
            {
                var result = Input.From(ListUsers(maxElements))
                .Apply(row =>
                {
                    rowCount++;
                    if (rowCount > throwAfter)
                        throw new InvalidDataException("problem");
                })
                .Execute();

                Assert.Fail("The run must have failed");
            }
            catch (EtlResultException ex)
            {
                Assert.AreEqual(1, ex.EtlResult.CountExceptions);
                var exc = ex.EtlResult.Exceptions.FirstOrDefault();
                Assert.IsInstanceOfType(exc, typeof(InvalidDataException));
            }                       
        }
    }
}
