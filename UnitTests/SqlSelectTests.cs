using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using TinySql;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using System.Data;
using TinySql.Serialization;

namespace UnitTests
{
    [TestClass]
    public class SqlSelectTests : BaseTest
    {
        [TestMethod]
        public void PopulateListClassFromResultTable()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.Select()
                .From("Account")
                .AllColumns(false)
                .SubSelect("Contact", "AccountID", "AccountID", null, null, "Contacts")
                .AllColumns(false)
                .Builder();
            Console.WriteLine(builder.ToSql());
            ResultTable result = builder.Execute();
            Console.WriteLine("ResulTable with {0} rows executed in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            g = StopWatch.Start();
            List<Account> accounts = TypeBuilder.PopulateObject<Account>(result);
            Console.WriteLine("List<Account> with {0} rows created from ResultTable executed in {1}ms", accounts.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            Console.WriteLine("List<Account> has a total of {0} contacts", accounts.SelectMany(x => x.Contacts).Count());
        }

        [TestMethod]
        public void PopulateClassFromResultTable()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.Select(1)
                .From("Account")
                .AllColumns(false)
                .SubSelect("Contact", "AccountID", "AccountID", null, null, "Contacts")
                .AllColumns(false)
                .Builder();
            Console.WriteLine(builder.ToSql());
            ResultTable result = builder.Execute();
            Console.WriteLine("ResulTable with {0} rows executed in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            g = StopWatch.Start();
            Account account = TypeBuilder.PopulateObject<Account>(result.First());
            Console.WriteLine("Account {0}-{1} with {2} Contacts created from RowData executed in {3}ms", account.AccountID, account.Name,account.Contacts.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
        }



        [TestMethod]
        public void TestHashCodes()
        {
            SqlBuilder Builder1 = SqlBuilder.Select().From("Contact").WithMetadata().InnerJoin("AccountID").Builder();
            SqlBuilder Builder2 = SqlBuilder.Select().From("Contact").WithMetadata().InnerJoin("AccountID").Builder();
            int h1 = Builder1.ToSql().GetHashCode();
            int h2 = Builder2.ToSql().GetHashCode();
            Assert.AreEqual(h1, h2, "The Builders are not equal");
            Console.WriteLine("Builder 1 hash: {0}. Builder 2 hash {1}", Builder1.ToSql().GetHashCode(), Builder2.ToSql().GetHashCode());
            Builder2.From("Contact").Where<decimal>("Contact", "ContactID", SqlOperators.Equal, 4109);
            h1 = Builder1.ToSql().GetHashCode();
            h2 = Builder2.ToSql().GetHashCode();
            Console.WriteLine("Builder 1 hash: {0}. Builder 2 hash {1}", h1, h2);
            Assert.AreNotEqual(h1, h2, "The Builders are not equal");

            


        }


        [TestMethod]
        public void ExecuteDeepParent()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.Select()
                .From("Contact")
                .AllColumns(false)
                .SubSelect("Account", "AccountID", "AccountID", null, null, "Account")
                .AllColumns(false)
                .ConcatColumns("Address", ", ", "Address1", "PostalCode", "City")
                .InnerJoin("Contact").On("AccountID", SqlOperators.Equal, "AccountID")
                .ToTable().Column("ContactID")
                .Builder();

            Console.WriteLine(builder.ToSql());
            ResultTable result = builder.Execute();
            Console.WriteLine("ResulTable with {0} rows executed in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            foreach (dynamic row in result.Where(x => x.Column<ResultTable>("Account").Count > 0).Take(50))
            {
                dynamic Parent = row.Column<ResultTable>("Account");
                if (Parent.Count > 0)
                {
                    Console.WriteLine("The Contact {0} is connected to the account {1} - {2}", row.Name, Parent[0].Name, Parent[0].Address);
                }
                else
                {
                    Console.WriteLine("The Contact {0} is not connected to an account", row.Name);
                }
            }
        }

        private ResultTable ExecuteDeepInternal()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.Select()
                .From("Account")
                .AllColumns(false)
                .SubSelect("Contact", "AccountID", "AccountID", null, null, "Contacts")
                .AllColumns(false)
                    .SubSelect("Activity", "ContactID", "ContactID", null, null, "Activities")
                    .AllColumns(false)
                    .InnerJoin("Checkkode").On("ActivityTypeID", SqlOperators.Equal, "CheckID")
                    .And<decimal>("CheckGroup", SqlOperators.Equal, 5)
                    .ToTable().Column("BeskrivelseDK", "ActivityType")
                .Builder.ParentBuilder.From("Contact")
                .SubSelect("CampaignActivity", "ContactID", "ContactID", null, null)
                .AllColumns(false)
                .InnerJoin("Checkkode").On("CampaignActivityTypeID", SqlOperators.Equal, "CheckID")
                .And<decimal>("CheckGroup", SqlOperators.Equal, 4)
                .ToTable().Column("BeskrivelseDK", "ActivityType")
                .Builder();

            Console.WriteLine(builder.ToSql());
            ResultTable result = builder.Execute();
            Console.WriteLine("ResulTable with {0} rows executed in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            return result;
        }



        [TestMethod]
        public void ExecuteDeepAndSerialize()
        {
            ResultTable result = ExecuteDeepInternal();
            string file = Path.Combine(Path.GetTempPath(), Path.GetRandomFileName() + ".json");
            Guid g = StopWatch.Start();
            File.WriteAllText(file, TinySql.Serialization.SerializationExtensions.ToJson<ResultTable>(result));
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "Results serialized in {0}ms"));
            g = StopWatch.Start();
            ResultTable deserialized = SerializationExtensions.FromJson<ResultTable>(File.ReadAllText(file));
            Console.WriteLine(StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds, "Results deserialized in {0}ms"));
            FileInfo fi = new FileInfo(file);
            Console.WriteLine("The File is {0:0.00}MB in size", fi.Length / (double)(1024 * 1024));
            fi.Delete();
            Assert.IsFalse(File.Exists(file));
            Assert.IsTrue(result.Count == deserialized.Count);
        }

        [TestMethod]
        public void ExecuteDeep()
        {
            ResultTable result = ExecuteDeepInternal();
            Console.WriteLine("The result has {0} Account rows", result.Count);
            Console.WriteLine("These accounts has a total of {0} Contact rows", result.SelectMany(x => x.Column<ResultTable>("Contacts")).Count());
            Console.WriteLine("These contacts has a total of {0} Activity rows", result.SelectMany(x => x.Column<ResultTable>("Contacts")).SelectMany(y => y.Column<ResultTable>("Activities")).Count());
            Console.WriteLine("These contacts has a total of {0} Campaign Activity rows", result.SelectMany(x => x.Column<ResultTable>("Contacts")).SelectMany(y => y.Column<ResultTable>("CampaignActivityList")).Count());
            foreach (dynamic row in result.Take(50))
            {
                ResultTable contacts = row.Contacts;
                Console.WriteLine("The account {0} has {1} Contacts", row.Name, contacts.Count);
                Console.WriteLine("The account {0} has a total of {1} Activities", row.Name, contacts.SelectMany(x => x.Column<ResultTable>("Activities")).Count());
            }

            Console.Write("Executing a second time with results in cahce:");
            result = ExecuteDeepInternal();




        }
        [TestMethod]
        public void SelectAllColumnsFromOneTable()
        {
            SqlBuilder builder = SqlBuilder.Select().From("Account").AllColumns(false).Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute();
            Console.WriteLine("ResulTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            g = StopWatch.Start();
            List<Account> list = builder.List<Account>(null, 30, true, true);
            Console.WriteLine("List<Account> with {0} rows executed in {1}s", list.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            list = null;
            result = null;
        }

        [TestMethod]
        public void SelectContacts()
        {
            Guid g = StopWatch.Start();
            DataTable dt = Data.All<Account>("Account", null, false, null, 30);
            List<Account> list = Data.All<Account>(null, null, true, null, 30, false, true);
            Dictionary<decimal, Account> dict = Data.All<decimal, Account>("AccountID");
            SqlBuilder builder = TypeBuilder.Select<Account>();
            builder.From("Account").FieldList.Remove(builder.From("Account").FieldList.First(x => x.Name.Equals("State")));
            builder.From("Account").FieldList.Remove(builder.From("Account").FieldList.First(x => x.Name.Equals("Datasource")));
            builder
                .From("Account")
                .InnerJoin("State").On("StateID", SqlOperators.Equal, "StateID").ToTable()
                .Column("Description", "State")
                .From("Account")
                .InnerJoin("Checkkode").On("DatasourceID", SqlOperators.Equal, "CheckID").And<decimal>("CheckGroup", SqlOperators.Equal, 7)
                .ToTable()
                .Column("BeskrivelseDK", "Datasource")
                .From("Account").OrderBy("Name", OrderByDirections.Asc)
            .Builder();
                
            list = builder.List<Account>();
            Console.WriteLine("All accounts selected with 5 different methods in {0}ms", StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
        }
        [TestMethod]
        public void ParallelSelectWithSubquery()
        {
            Guid g = StopWatch.Start();
            int loops = 80;
            SqlBuilder builder = SqlBuilder.Select()
                .From("Account")
                .AllColumns(false)
                .SubSelect("Contact", "AccountID", "AccountID", null)
                .AllColumns(false)
                .Builder();
            Console.WriteLine(builder.ToSql());
            ResultTable result = builder.Execute();
            double one = StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds);
            int count = result.Count + result.SelectMany(x => x.Column<ResultTable>("ContactList")).Count();

            g = StopWatch.Start();
            int total = 0;
            ParallelLoopResult r = Parallel.For(0, loops, new ParallelOptions() { MaxDegreeOfParallelism = 8 }, a => { total += SelectWithSubqueryInternal(); });
            double twenty = StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds);
            Console.WriteLine("{2} selects parallel executed with a total of {3} rows in {0:0.00}ms. Estimated {1:0.00}ms", twenty, one * loops, loops, loops * count);
            Assert.IsTrue(r.IsCompleted);

        }

        private int SelectWithSubqueryInternal()
        {
            Guid g = StopWatch.Start();
            SqlBuilder builder = SqlBuilder.Select()
                .From("Account")
                .AllColumns(false)
                .SubSelect("Contact", "AccountID", "AccountID", null)
                .Columns("ContactID", "Name", "Title")
                .Builder();

            ResultTable result = builder.Execute();
            Console.WriteLine("{0} rows executed from Thread {1}", result.Count + result.SelectMany(x => x.Column<ResultTable>("ContactList")).Count(), System.Threading.Thread.CurrentThread.ManagedThreadId);
            return result.Count;
        }


        [TestMethod]
        public void SelectWithSubQuery()
        {
            SqlBuilder builder = SqlBuilder.Select(100)
                .From("Account")
                .AllColumns(false)
                .SubSelect("Contact", "AccountID", "AccountID", null)
                .Columns("ContactID", "Name", "Title")
                .Builder();


            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            System.Data.DataSet result = builder.DataSet();
            Console.WriteLine("Dataset with {0} tables executed in {1}ms", result.Tables.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            for (int i = 0; i < result.Tables.Count; i++)
            {
                Console.WriteLine("Table {0} contains {1} rows", i, result.Tables[i].Rows.Count);
            }
        }



        [TestMethod]
        public void SelectAllBusinessEntitiesWithNestedSubQueriesAndJoin()
        {
            SqlBuilder builder = SqlBuilder.Select()
                .From("Account")
                .Columns("AccountID", "Name", "Address1", "PostalCode", "City")
                .SubSelect("Contact", "AccountID", "AccountID")
                .Columns("ContactID", "Name", "Title", "Telephone", "Mobile", "WorkEmail", "PrivateEmail")
                .InnerJoin("ListMember").On("ContactID", SqlOperators.Equal, "ContactID").ToTable()
                .Columns("ListMemberID", "ListMemberStatusID")
                .From("Contact")
                .SubSelect("ListMember", "ContactID", "ContactID")
                .Columns("ContactID", "ListMemberID", "ListmemberStatusID", "ListID")
                .SubSelect("List", "ListID", "ListID")
                .Columns("ListID", "Name")
                .Builder();

            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            System.Data.DataSet result = builder.DataSet();
            Console.WriteLine("Dataset with {0} tables executed in {1}ms", result.Tables.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            for (int i = 0; i < result.Tables.Count; i++)
            {
                Console.WriteLine("Table {0} contains {1} rows", i, result.Tables[i].Rows.Count);
            }
        }


        [TestMethod]
        public void SelectWithAliasInWhereClause()
        {
            SqlBuilder builder = SqlBuilder.Select()
                .From("account", null)
                .Into("tempPerson")
                .Column("AccountID", "ID")
                .Columns("Name", "Address1", "PostalCode", "City")
                .OrderBy("ID", OrderByDirections.Desc)
                .Where<int>("account", "ID", SqlOperators.LessThan, 1000)
                .AndGroup()
                    .And<string>("account", "Name", SqlOperators.StartsWith, "A")
                    .Or<string>("account", "City", SqlOperators.StartsWith, "A")
                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(120);
            Console.WriteLine("ResultTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
        }
        [TestMethod]
        public void Select2000AccountsIntoTempTableWithOutput()
        {
            SqlBuilder builder = SqlBuilder.Select(2000)
                .From("Account")
                .Into("tempPerson")
                .Columns("AccountID", "Name", "Address1")
                .ConcatColumns("Address", "\r\n", "Address1", "PostalCode", "City")
                .OrderBy("AccountID", OrderByDirections.Desc)
                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(120);
            Console.WriteLine("ResultTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
        }
        [TestMethod]
        public void SelectAllColumnsFromOneTableOrderedByTwoFields()
        {

            SqlBuilder builder = SqlBuilder.Select(2000)
                .From("Account")
                .AllColumns(false)
                .OrderBy("City", OrderByDirections.Desc).OrderBy("Name", OrderByDirections.Asc)
                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(120);
            Console.WriteLine("ResulTable with {0} rows executed in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            foreach (dynamic row in result.Take(10))
            {
                Console.WriteLine("{0} {1} - Ordered by City DESC, Name ASC", row.Name, row.City);
            }
        }


        [TestMethod]
        public void SelectValueColumnFromOneTable()
        {
            SqlBuilder builder = SqlBuilder.Select().From("Account", null).AllColumns(false)
                .Column<int>(22, "Age")
                .Column<string>(Guid.NewGuid().ToString(), "UniqueID")
                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(120);
            Console.WriteLine("ResulTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            g = StopWatch.Start();
            List<Account> list = builder.List<Account>(null, 30, true, true);
            Console.WriteLine("List<Account> with {0} rows executed in {1}s", list.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            list = null;
            result = null;
        }

        [TestMethod]
        public void SelectFunctionColumnsFromOneTable()
        {
            SqlBuilder builder = SqlBuilder.Select(20).From("Account", null).AllColumns(false)
                .Fn()
                    .GetDate("Today")
                    .Concat("My Name",
                        ConstantField<string>.Constant("Michael"),
                        ConstantField<string>.Constant(" "),
                        ConstantField<string>.Constant("Randrup")
                        )
                .ToTable()
                .Column<string>(Guid.NewGuid().ToString(), "UniqueID")
                .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute(120);
            Console.WriteLine("ResulTable with {0} rows executed in {1}s", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            g = StopWatch.Start();
            List<Account> list = builder.List<Account>(null, 30, true, true);
            Console.WriteLine("List<Account> with {0} rows executed in {1}s", list.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Seconds));
            list = null;
            result = null;
        }


        [TestMethod]
        public void SelectAllAccountsIntoCustomDictionary()
        {
            SqlBuilder builder = SqlBuilder.Select().From("Account").AllColumns(false)
                    .InnerJoin("State", null).On("StateID", SqlOperators.Equal, "StateID")
                    .ToTable()
                    .Column("Description", "State")
                .From("Account")
                    .InnerJoin("Checkkode", null).On("DatasourceID", SqlOperators.Equal, "CheckID")
                    .And<decimal>("CheckGroup", SqlOperators.Equal, 7).ToTable()
                    .Column("BeskrivelseDK", "Datasource")
            .Builder();
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            MyDictionary result = builder.Dictionary<decimal, Account, MyDictionary>("AccountID");
            Console.WriteLine("MyDictionary<int, Account> with {0} rows executed in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            g = StopWatch.Start();
            result = null;
        }

        [TestMethod]
        public void SelectAccountsWithCustomAliasesIntoResultTable()
        {

            SqlBuilder builder = SqlBuilder.Select().From("Account", "a").AllColumns(false)
                    .InnerJoin("State", "b").On("StateID", SqlOperators.Equal, "StateID")
                    .ToTable()
                    .Column("Description", "State")
                .From("a")
                    .InnerJoin("Checkkode", "c").On("DatasourceID", SqlOperators.Equal, "CheckID")
                    .And<decimal>("CheckGroup", SqlOperators.Equal, 7).ToTable()
                    .Column("BeskrivelseDK", "Datasource")
            .Builder;
            Console.WriteLine(builder.ToSql());
            Guid g = StopWatch.Start();
            ResultTable result = builder.Execute();
            Console.WriteLine("ResultTable with {0} rows executed in {1}ms", result.Count, StopWatch.Stop(g, StopWatch.WatchTypes.Milliseconds));
            g = StopWatch.Start();
            result = null;
        }



        public class MyDictionary : Dictionary<decimal, Account>
        {

        }




    }
}
