using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;
using TinySql.Serialization;

namespace UnitTests
{
    [TestClass]
    public class StoredProcedureTests : BaseTest
    {
        private SqlBuilder GetInsertUpdateBuilder(decimal AccountID = 0, string Address1 = "Procedure Address")
        {
            //return SqlBuilder.StoredProcedure("prcAccountSave")
            //    .Parameter<decimal>("AccountID", System.Data.SqlDbType.Decimal, AccountID)
            //    .Parameter<string>("Name", System.Data.SqlDbType.VarChar, "Applications A/S", 200)
            //    .Parameter<string>("Address1", System.Data.SqlDbType.VarChar, Address1, 100)
            //    .Parameter<string>("Address2", System.Data.SqlDbType.VarChar, null, 100)
            //    .Parameter<string>("Address3", System.Data.SqlDbType.VarChar, null, 100)
            //    .Parameter<string>("PostalCode", System.Data.SqlDbType.VarChar, "1165", 10)
            //    .Parameter<string>("City", System.Data.SqlDbType.VarChar, "Copenhagen K", 50)
            //    .Parameter<string>("Telephone", System.Data.SqlDbType.VarChar, "Copenhagen K", 20)
            //    .Parameter<string>("Telefax", System.Data.SqlDbType.VarChar, null, 20)
            //    .Parameter<string>("Web", System.Data.SqlDbType.VarChar, "http://www.applications.dk", 50)
            //    .Parameter<decimal>("AccountTypeID", System.Data.SqlDbType.Decimal, 1, -1, 0, 18)
            //    .Parameter<decimal>("DatasourceID", System.Data.SqlDbType.Decimal, 1, -1, 0, 18)
            //    .Parameter<decimal>("OwningUserID", System.Data.SqlDbType.Decimal, 2, -1, 0, 18)
            //    .Parameter<decimal>("OwningBusinessUnitID", System.Data.SqlDbType.Decimal, 2, -1, 0, 18)
            //    .Parameter<decimal>("StateID", System.Data.SqlDbType.Decimal, 1, -1, 0, 18)
            //    .Parameter<decimal>("UserID", System.Data.SqlDbType.Decimal, 2, -1, 0, 18)
            //    .Output<decimal>("retval", System.Data.SqlDbType.Decimal, -1, 0, 18)
            //    .Builder();

            return SqlBuilder.StoredProcedure("prcAccountSave")
                .Parameter<decimal>("Account","AccountID", AccountID)
                .Parameter<string>("Account","Name", "Applications A/S")
                .Parameter<string>("Account","Address1", Address1)
                .Parameter<string>("Account","Address2", null)
                .Parameter<string>("Account","Address3", null)
                .Parameter<string>("Account","PostalCode", "1165")
                .Parameter<string>("Account","City", "Copenhagen K")
                .Parameter<string>("Account","Telephone", "Copenhagen K")
                .Parameter<string>("Account","Telefax", null)
                .Parameter<string>("Account","Web", "http://www.applications.dk")
                .Parameter<decimal>("Account","AccountTypeID", 1)
                .Parameter<decimal>("Account","DatasourceID", 1)
                .Parameter<decimal>("Account","OwningUserID", 2)
                .Parameter<decimal>("Account","OwningBusinessUnitID", 2)
                .Parameter<decimal>("Account","StateID", 1)
                .Parameter<decimal>("SystemUser","UserID", 2,"SystemUserID")
                .Output<decimal>("retval", System.Data.SqlDbType.Decimal, -1, 0, 18)
                .Builder();



        }

        private void DeleteOneAccount(decimal ID)
        {
            SqlBuilder builder = SqlBuilder.Delete()
               .From("Account")
               .Where<decimal>("Account", "AccountID", SqlOperators.Equal, ID)
               .Builder();
            Console.WriteLine(builder.ToSql());

            int i = builder.ExecuteNonQuery();
            Assert.IsTrue(i == 1, "The Account {0} could not be deleted", ID);
        }

        [TestMethod]
        public void InsertOneAccountAsStoredProcedureText()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = GetInsertUpdateBuilder();
            ResultTable result = builder.Execute(30,false);
            Assert.IsTrue(result.Count == 1, "The insert procedure did not return 1 row");
            decimal ID = Convert.ToDecimal(builder.Procedure.Parameters.First(x => x.Name.Equals("retval")).Value);
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "One account inserted in {0}ms"));
            Assert.IsTrue(ID > 0, "The Account was not inserted");
            Console.WriteLine(SerializationExtensions.ToJson<RowData>(result[0], true));

            g = StopWatch.Start();
            builder = GetInsertUpdateBuilder(ID, "Nørregade 28D");
            result = builder.Execute(30, false);
            Assert.IsTrue(result.Count == 1, "The update procedure did not return 1 row");
            decimal ID2 = Convert.ToDecimal(builder.Procedure.Parameters.First(x => x.Name.Equals("retval")).Value);
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "One account updated in {0}ms"));
            Assert.AreEqual<decimal>(ID, ID2, "The Insert/update IDs do not match {0} != {1}", ID, ID2);
            builder = SqlBuilder.Select()
                .From("Account").AllColumns(false)
                .Where<decimal>("Account", "AccountID", SqlOperators.Equal, ID2)
                .Builder();
            result = builder.Execute(30, false);
            Assert.IsTrue(result.Count == 1, "The updated account {0} could not be retrieved", ID2);
            Console.WriteLine(SerializationExtensions.ToJson<RowData>(result[0], true));
            DeleteOneAccount(ID2);

        }
        [TestMethod]
        public void InsertOneAccountAsStoredProcedure()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = GetInsertUpdateBuilder();

            Console.WriteLine(builder.ToSql());
            int i = new SqlBuilder[] { builder }.ExecuteNonQuery();
            Assert.IsTrue(i == 1, "The insert procedure did not return 1 row");
            decimal ID = Convert.ToDecimal(builder.Procedure.Parameters.First(x => x.Name.Equals("retval")).Value);
            Assert.IsTrue(ID > 0, "The Account was not inserted");
            Console.WriteLine(string.Format("An account with the ID {0} was inserted in {1}ms", ID, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds)));

            g = StopWatch.Start();
            SqlBuilder select = SqlBuilder.Select()
                .From("Account")
                .AllColumns(false)
                .Where<decimal>("Account", "AccountID", SqlOperators.Equal, ID)
                .Builder();
            Console.WriteLine(select.ToSql());
            ResultTable result = select.Execute();
            Assert.IsTrue(result.Count == 1, "The Account could not be retrieved after insert");
            dynamic row = result.First();
            Console.WriteLine("Account ID {0}: {1} {2} retrieved in {3}ms", row.AccountID, row.Name, row.Address1, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));

            g = StopWatch.Start();
            builder = GetInsertUpdateBuilder(ID, "Nørregade 28D");
            i = new SqlBuilder[] { builder }.ExecuteNonQuery();
            Assert.IsTrue(i == 1, "The update procedure did not return 1 row");
            decimal ID2 = Convert.ToDecimal(builder.Procedure.Parameters.First(x => x.Name.Equals("retval")).Value);
            Assert.AreEqual<decimal>(ID, ID2);
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "The Account was updated in {0}ms"));
            g = StopWatch.Start();
            result = select.Execute();
            Assert.IsTrue(result.Count == 1, "The Account could not be retrieved after update");
            row = result.First();
            Console.WriteLine("Account ID {0}: {1} {2} retrieved in {3}ms", row.AccountID, row.Name, row.Address1, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));

            builder = SqlBuilder.Delete()
                .From("Account")
                .Where<decimal>("Account", "AccountID", SqlOperators.Equal, ID2)
                .Builder();

            Console.WriteLine(builder.ToSql());

            i = new SqlBuilder[] { builder }.ExecuteNonQuery();
            Assert.IsTrue(i == 1, "The Account could not be deleted");

        }
    }
}
