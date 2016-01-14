using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;
using System.Collections.Generic;

namespace UnitTests
{
    [TestClass]
    public class SqlInsertTests : BaseTest
    {
        static decimal FirstInsertedId = 99999999;

        [TestMethod]
        public void InsertAccountFromObject()
        {
            Guid g = StopWatch.Start();
            Account acc = new Account()
            {
                AccountID = 88888888,
                Name = "Test Account",
                AccountTypeID = 1,
                DatasourceID = 1,
                StateID = 1,
                CreatedBy = 1,
                CreatedOn = DateTime.Now,
                ModifiedBy = 1,
                ModifiedOn = DateTime.Now,
                OwningUserID = 1,
                OwningBusinessUnitID = 1
            };

            SqlBuilder builder = TypeBuilder.Insert<Account>(acc);
            Console.WriteLine(builder.ToSql());
            ResultTable result = builder.Execute();
            Assert.IsTrue(result.Count == 1);
            decimal i = result.First().Column<decimal>("AccountID");
            Console.WriteLine("Inserted Account {0}", i);


            SqlBuilder b1 = SqlBuilder.Delete()
                .From("account", null)
                .Where<decimal>("account", "AccountID", SqlOperators.Equal, i)
                .Builder;


            i = new SqlBuilder[] { b1 }.ExecuteNonQuery();
            Console.WriteLine("{0} Accounts deleted", i);
            Assert.IsTrue(i == 1); 

        }


        [TestMethod]
        public void InsertOneAccount()
        {
            Assert.IsTrue(InsertOneAccountInternal() == 1);
        }
        
        

        public int InsertOneAccountInternal(bool WriteSql = true)
        {
            Guid g = StopWatch.Start();

            string id = DateTime.Now.Ticks.ToString();
            SqlBuilder builder = SqlBuilder.Insert()
                .Into("account")
                .Value<string>("Name", "Test Account " + id, System.Data.SqlDbType.VarChar)
                .Value<string>("Address1", "Address1 " + id, System.Data.SqlDbType.VarChar)
                .Value<string>("Address2", "Address2 " + id, System.Data.SqlDbType.VarChar)
                .Value<string>("Address3", "Address3 " + id, System.Data.SqlDbType.VarChar)
                .Value<string>("PostalCode", "1165", System.Data.SqlDbType.VarChar)
                .Value<string>("City", "City " + id, System.Data.SqlDbType.VarChar)
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
                .Builder;
            if (WriteSql)
            {
                Console.WriteLine(builder.ToSql());
            }
            g = StopWatch.Start();
            ResultTable result = builder.Execute();
            if (FirstInsertedId == 99999999)
            {
                FirstInsertedId = result.First().Column<decimal>("AccountID");
            }
            if (WriteSql)
            {
                Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "One account inserted in {0}ms"));
            }
            Assert.IsTrue(result.Count == 1);
            return result.Count;
        }

        [TestMethod]
        public void Insert1000Accounts()
        {
            int num = 0;
            Guid g = StopWatch.Start();
            for (int i = 0; i < 1000; i++)
            {
                num += InsertOneAccountInternal(i < 1);
            }
            Console.WriteLine("{0} accounts inserted in {1:0.00}s", num, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            Assert.IsTrue(num == 1000);
            DeleteInsertedAccounts();
        }

        [TestMethod]
        public void Insert1000AccountsInOneBatch()
        {
            Guid g = StopWatch.Start();
            List<SqlBuilder> Builders = new List<SqlBuilder>();
            InsertOneAccountInternal(false);
            for (int i = 0; i < 1000; i++)
            {
                string id = DateTime.Now.Ticks.ToString();
                SqlBuilder builder = SqlBuilder.Insert()
                    .Into("account")
                    .Value<string>("Name", "Test Account " + id, System.Data.SqlDbType.VarChar)
                    .Value<string>("Address1", "Address1 " + id, System.Data.SqlDbType.VarChar)
                    .Value<string>("Address2", "Address2 " + id, System.Data.SqlDbType.VarChar)
                    .Value<string>("Address3", "Address3 " + id, System.Data.SqlDbType.VarChar)
                    .Value<string>("PostalCode", "1165", System.Data.SqlDbType.VarChar)
                    .Value<string>("City", "City " + id, System.Data.SqlDbType.VarChar)
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
                    .Builder;
                Builders.Add(builder);
            }
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "1000 Builders created in {0}ms"));
            g = StopWatch.Start();
            int result = Builders.ToArray().ExecuteNonQuery();
            Console.WriteLine("{0} Rows affected in {1}ms", result, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            Assert.IsTrue(result == 1000);
            DeleteInsertedAccounts();
        }


        public void InsertWithRelated()
        {
            SqlBuilder builder = SqlBuilder.Insert().Into("Account")
                .Value<string>("Name", "Test account", System.Data.SqlDbType.VarChar, 200)
                .Builder();

                


        }

        public void DeleteInsertedAccounts()
        {
            SqlBuilder b1 = SqlBuilder.Delete()
                .From("account", null)
                .Where<decimal>("account", "AccountID", SqlOperators.GreaterThanEqual, FirstInsertedId)
                .Builder;
            Console.WriteLine(b1.ToSql());
            Guid g = StopWatch.Start();

            int i = new SqlBuilder[] { b1 }.ExecuteNonQuery();
            Console.WriteLine("{0} Accounts deleted", i);
            Assert.IsTrue(i >= 1000);
            FirstInsertedId = 99999999;
        }

    }
}
