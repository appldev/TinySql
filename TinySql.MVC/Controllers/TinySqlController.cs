using System;
using System.Linq;
using System.Web.Mvc;
using TinySql.Serialization;
using TinySql.Metadata;
using TinySql.UI;

namespace TinySql.MVC.Controllers
{
    public class TinySqlController : Controller
    {
        // GET: TinySql
        public ActionResult Index()
        {
            return View();
        }

        public PartialViewResult Edit(string Id, string Table, ListTypes ListType, string ListName)
        {

            MetadataTable table = SqlBuilder.DefaultMetadata.FindTable(Table);
            // SqlBuilder builder = table.ToSqlBuilder(ListType != ListTypes.Custom ? ListType.ToString() : ListName);
            SqlBuilder builder = table.ToSqlBuilder("");
            builder.BaseTable().WithMetadata().WherePrimaryKey(new object[] { Id });

            ResultTable result = builder.Execute();
            
            Form model = FormFactory.Default.BuildForm(builder);
            model.Initialize(result.First());
            return PartialView(model.EditDialogViewUrl,model);
        }

        

        [HttpPost]
        //public ContentResult Save(FormCollection Model, string Table, ListTypes ListType, string ListName)
        
        // public ContentResult Save(SaveModel Model)
        public ContentResult Save(string rowData, string Table, ListTypes ListType, string ListName)
        {
            RowData row = SerializationExtensions.FromJson<RowData>(rowData);
            row.LoadMetadata();
            row.LoadMissingColumns<bool>();
            SqlBuilder builder = row.Update(false, true);
            ResultTable result = builder.Execute();
            if (result.Count == 1)
            {
                builder = row.Select(ListType != ListTypes.Custom ? ListType.ToString() : ListName);
                //builder = row.Metadata.ToSqlBuilder(ListType != ListTypes.Custom ? ListType.ToString() : ListName);
                //builder.WhereConditions = row.PrimaryKey(builder);
                result = builder.Execute(30, false, ResultTable.DateHandlingEnum.ConvertToDate);
                if (result.Count == 1)
                {
                    return Content(SerializationExtensions.ToJson<dynamic>(result.First()), "application/json");
                }

                //// object PK = result.First().Column(mt.PrimaryKey.Columns.First().Name);
                
                //Builder.BaseTable().WithMetadata().WherePrimaryKey(new object[] { (object)PK });
                //ResultTable updated = Builder.Execute(30, false, ResultTable.DateHandlingEnum.ConvertToDate);
                //if (updated.Count == 1)
                //{
                //    return Content(SerializationExtensions.ToJson<dynamic>(updated.First()), "application/json");
                //}

            }




            //// Retrieve by Primary key
            //MetadataTable mt = SqlBuilder.DefaultMetadata.FindTable(Table);
            //List<object> PKs = new List<object>();
            //foreach (MetadataColumn mc in mt.PrimaryKey.Columns)
            //{
            //    PKs.Add(Model[Table + "_" + mc.Name]);
            //}
            //// Create an empty row with the primary key set
            //RowData row = RowData.Create(mt, true, PKs.ToArray());

            //// Change the row
            //foreach (string key in Model.Keys)
            //{
            //    if (!key.StartsWith("__"))
            //    {
            //        string ColumnName = key.Replace(Table, "").Replace("_", "");
            //        MetadataColumn mc;
            //        if (mt.Columns.TryGetValue(ColumnName, out mc))
            //        {
            //            if (!mc.IsReadOnly)
            //            {
            //                // row.Column(mc.Name, (object)Model[key]);
            //                row.Column(mc.Name, Model[key]);
            //            }
            //        }
            //    }
            //}

            //// Build SQL and update
            //SqlBuilder builder = row.Update(true, true);
            //ResultTable result = builder.Execute(30, false);


            //if (result.Count == 1)
            //{
            //    object PK = result.First().Column(mt.PrimaryKey.Columns.First().Name);
            //    SqlBuilder Builder = mt.ToSqlBuilder(ListType != ListTypes.Custom ? ListType.ToString() : ListName);
            //    Builder.BaseTable().WithMetadata().WherePrimaryKey(new object[] { (object)PK });
            //    ResultTable updated = Builder.Execute(30, false, ResultTable.DateHandlingEnum.ConvertToDate);
            //    if (updated.Count == 1)
            //    {
            //        return Content(SerializationExtensions.ToJson<dynamic>(updated.First()), "application/json");
            //    }

            //}

            return Content("");


        }


        [HttpPost]
        public ContentResult Update(string rowData)
        {
            RowData row = SerializationExtensions.FromJson<RowData>(rowData);
            row.LoadMetadata();
            row.Column("Name", "Random " + Guid.NewGuid().ToString());
            SqlBuilder builder = row.Update(false,true);

            ResultTable result = builder.Execute(30, false);
            if (result.Count == 1)
            {
                 builder = SqlBuilder.Select()
                .From("Contact")
                .Column("ContactID")
                .Column("Name")
                .Column("Telephone")
                .Column("WorkEmail")
                .Column("ModifiedOn")
                .WithMetadata().InnerJoin("AccountID")
                .Column("Name", "AccountName")
                .From("Contact")
                .Where<decimal>("Contact", "ContactID", SqlOperators.Equal,result.First().Column<decimal>("ContactID"))
                .Builder();
                
                result = builder.Execute(30, false, ResultTable.DateHandlingEnum.ConvertToDate);
                row = result.First();
                return Content(SerializationExtensions.ToJson<dynamic>(row), "application/json");
            }
            return Content("Hmmmm...?","application/text");
        }

        [AcceptVerbs( HttpVerbs.Get)]
        public ContentResult Rows(string TableName, ListTypes ListType, string ListName)
        {
            MetadataTable mt = SqlBuilder.DefaultMetadata.FindTable(TableName);
            SqlBuilder Builder = mt.ToSqlBuilder(ListType != ListTypes.Custom ? ListType.ToString() : ListName);

            ResultTable result = Builder.Execute(30, false, ResultTable.DateHandlingEnum.ConvertToDate);
            return Content(SerializationExtensions.ToJson<dynamic>(result), "application/json");

        }

        [AcceptVerbs(HttpVerbs.Get)]
        public PartialViewResult List(string TableName, ListTypes ListType, string ListName)
        {
            ListBuilder model = ListFactory.Default.BuildList(TableName, ListType, null, ListName);
            return PartialView(model.ListViewUrl, model);
        }

    }
}