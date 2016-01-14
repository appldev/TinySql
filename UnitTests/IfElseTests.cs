using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;

namespace UnitTests
{
    [TestClass]
    public class IfElseTests : BaseTest
    {
        [TestMethod]
        public void TestSimpleIf()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.If()
                .Conditions.AndExists("Account")
                .And<decimal>("Account", "AccountID", SqlOperators.Equal, 526)
                .Begin(SqlBuilder.StatementTypes.Select)
                .From("Account")
                .AllColumns(false)
                .Where<decimal>("Account", "AccountID", SqlOperators.Equal, 526)
                .Builder.End();

            Console.WriteLine(builder.ToSql());

            ResultTable result = builder.Execute();

            Console.WriteLine(string.Format("{0} rows retrieved in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds)));
            Assert.IsTrue(result.Count == 1);

        }

        private ResultTable InsertOrUpdate(decimal Id)
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.If()
                .Conditions.AndExists("Account")
                .And<decimal>("Account", "AccountID", SqlOperators.Equal, Id)
                .Begin(SqlBuilder.StatementTypes.Update).Table("Account")
                    .Output()
                        .Column("AccountID", System.Data.SqlDbType.Decimal)
                    .UpdateTable()
                    .Set<string>("Name", "Name Updated " + DateTime.Now.ToString(), System.Data.SqlDbType.VarChar, 200)
                    .Where<decimal>("Account","AccountID", SqlOperators.Equal, Id)
                .Builder.End()
                .Else().Begin(SqlBuilder.StatementTypes.Insert).Into("Account")
                    .Value<string>("Name", "Test Account", System.Data.SqlDbType.VarChar)
                    .Value<string>("Address1", "Address1", System.Data.SqlDbType.VarChar)
                    .Value<string>("Address2", "Address2", System.Data.SqlDbType.VarChar)
                    .Value<string>("Address3", "Address3", System.Data.SqlDbType.VarChar)
                    .Value<string>("PostalCode", "1165", System.Data.SqlDbType.VarChar)
                    .Value<string>("City", "City", System.Data.SqlDbType.VarChar)
                    .Value<string>("Telephone", "500-500-2015", System.Data.SqlDbType.VarChar)
                    .Value<string>("Telefax", "500-500-2015", System.Data.SqlDbType.VarChar)
                    .Value<string>("Web", "http://www.company.com", System.Data.SqlDbType.VarChar)
                    .Value<decimal>("AccountTypeID", 1, System.Data.SqlDbType.Decimal)
                    .Value<decimal>("DataSourceID", 1, System.Data.SqlDbType.Decimal)
                    .Value<decimal>("StateID", 1, System.Data.SqlDbType.Decimal)
                    .Value<decimal>("CreatedBy", 1, System.Data.SqlDbType.Decimal)
                    .Value<decimal>("ModifiedBy", 1, System.Data.SqlDbType.Decimal)
                    .Value<DateTime>("CreatedOn", DateTime.Now, System.Data.SqlDbType.DateTime)
                    .Value<DateTime>("ModifiedOn", DateTime.Now, System.Data.SqlDbType.DateTime)
                    .Value<decimal>("OwningUserID", 1, System.Data.SqlDbType.Decimal)
                    .Value<decimal>("OwningBusinessUnitID", 1, System.Data.SqlDbType.Decimal)
                    .Output()
                        .Column("AccountID", System.Data.SqlDbType.Decimal)
                .Builder.End();

            Console.WriteLine(builder.ToSql());
            ResultTable result = builder.Execute();
            Console.WriteLine(string.Format("{0} rows inserted or updated in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds)));
            Assert.IsTrue(result.Count == 1);
            return result;
        }

        [TestMethod]
        public void TestIfElseInsertUpdate()
        {
            decimal Id = 0;
            Console.WriteLine("Pass 1/2: Inserting Account ID {0}", Id);
            ResultTable result = InsertOrUpdate(Id);
            Console.WriteLine("{0} rows returned", result.Count);
            Assert.IsTrue(result.Count == 1);
            Id = result.First().Column<decimal>("AccountID");
            Console.WriteLine("Updating returned Account ID {0}", Id);
            result = InsertOrUpdate(Id);
            Assert.IsTrue(result.Count == 1);
            decimal Id2 = result.First().Column<decimal>("AccountID");
            Console.WriteLine("Pass 2/2: Account ID {0} returned", Id);
            Assert.IsTrue(Id.Equals(Id2));
        }
    }
}
