using System;
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;
using System.Collections.Generic;

namespace UnitTests
{
    [TestClass]
    public class SqlUpdateTests : BaseTest
    {
        [TestMethod]
        public void UpdateAccountFromObject()
        {
            SqlBuilder builder = SqlBuilder.Select()
                .From("Account")
                .AllColumns()
                .Where<decimal>("account", "AccountID", SqlOperators.Equal, 526)
                .Builder();

            Account account = builder.FirstOrDefault<Account>();
            account.ModifiedOn = DateTime.Now;
            string OldName = account.Name;
            account.Name = Guid.NewGuid().ToString();
            Console.WriteLine("Updating account ID 526 from Name {0} to {1}", OldName, account.Name);
            Guid g = StopWatch.Start();
            builder = TypeBuilder.Update<Account>(account,"Account");
            Console.WriteLine(builder.ToSql());
            int i = builder.ExecuteNonQuery();
            Assert.IsTrue(i == 1, "Account 526 was NOT updated");
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "Account 526 updated to the new name in {0}ms"));
            g = StopWatch.Start();
            account.Name = OldName;
            builder = TypeBuilder.Update<Account>(account, properties: new string[] { "Name", "ModifiedOn" });
            i = builder.ExecuteNonQuery();
            Assert.IsTrue(i == 1, "Account 526 was NOT updated back to the original name again");
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "Account 526 updated back to the original name again in {0}ms"));
        }


        [TestMethod]
        public void UpdateAccountWithOutputResults()
        {
            string NewName = Guid.NewGuid().ToString();
            SqlBuilder builder = SqlBuilder.Update()
                .Table("account")
                    .Output()
                    .Column("AccountID", System.Data.SqlDbType.Decimal)
                    .Column("Name", System.Data.SqlDbType.VarChar, 50)
                .UpdateTable()
                .Set<string>("Name", NewName, System.Data.SqlDbType.VarChar)
                .Where<decimal>("account", "AccountID", SqlOperators.Equal, 526)

                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            List<Account> Accounts = builder.List<Account>();
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "One person updated and retrieved as List<T> in {0}ms"));
            Assert.IsTrue(Accounts.Count == 1);
            Assert.AreEqual<string>(NewName, Accounts.First().Name);
        }

        [TestMethod]
        public void UpdateAccountWithJoinAndOutputResults()
        {
            string NewTitle = Guid.NewGuid().ToString();
            SqlBuilder builder = SqlBuilder.Update()
                .Table("Account")
                    .Output()
                    .Column("AccountID", System.Data.SqlDbType.Decimal)
                    .Column("Name", System.Data.SqlDbType.VarChar, 50)
                .UpdateTable()
                .Set<string>("Name", NewTitle, System.Data.SqlDbType.VarChar)
                .InnerJoin("Systemuser").On("OwningUserID", SqlOperators.Equal, "SystemUserID")
                .ToTable()
                .Where<decimal>("Account", "AccountID", SqlOperators.Equal, 526)
                .And<decimal>("SystemUser", "SystemUserID", SqlOperators.Equal, 2)
                
                //.ToTable()
                //.Where<decimal>("SystemUser", "SystemUserID", SqlOperators.Equal, 1)
                

                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            List<Account> Accounts = builder.List<Account>();
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "One account updated and retrieved as List<T> in {0}ms"));
            Assert.IsTrue(Accounts.Count == 1);
            Assert.AreEqual<string>(NewTitle, Accounts.First().Name);
        }

        [TestMethod]
        public void AutoUpdateFromRowDataObject()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.Select().WithMetadata(true,SetupData.MetadataFileName)
            .From("Account")
            .AllColumns(false)
            .Where<decimal>("Account", "AccountID", SqlOperators.Equal, 526)
            .Builder;
            Console.WriteLine(builder.ToSql());
            ResultTable r = builder.Execute();
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "1 Account selected in {0}ms"));
            g = StopWatch.Start();
            Assert.IsTrue(r.Count == 1,"Executed 1 account");
            RowData row = r.First();
            row.Column("Name", Guid.NewGuid().ToString());
            builder = SqlBuilder.Update().Update(row, new string[] { "AccountID", "Name" });
            Console.WriteLine(builder.ToSql());
            r = builder.Execute();
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "1 Account updated in {0}ms"));
            row.AcceptChanges();
            Assert.IsTrue(r.First().Column<string>("Name") == row.Column<string>("Name"),"Names are equal");
            Assert.IsFalse(row.HasChanges,"The row does not have changes");
            


                


        }

    }
}
