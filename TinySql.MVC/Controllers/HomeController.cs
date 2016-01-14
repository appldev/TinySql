using System.Linq;
using System.Web.Mvc;
using TinySql.Metadata;
using TinySql.UI;



namespace TinySql.MVC.Controllers
{
    public class HomeController : Controller
    {
        public ActionResult Contacts()
        {
            //SqlBuilder builder = SqlBuilder.Select()
            //    .From("Contact")
            //    .AllColumns()
            //    .Builder();
            
            // List<Expression<Func<TClass, TProperty>>> list = new List<Expression<Func<TClass, TProperty>>>();



            
            return View();
        }

        public ActionResult EditForm()
        {
            MetadataTable table = SqlBuilder.DefaultMetadata.FindTable("Contact");
            
            // Form model = FormFactory.Default.GetForm(table, FormTypes.Primary);
            //SqlBuilder builder = SqlBuilder.Select()
            //    .From("Contact").AllColumns()
            //    .Where<decimal>("Contact","ContactID", SqlOperators.Equal,1403)
            //    .Builder();

            SqlBuilder builder = SqlBuilder.Select()
                .From("Contact").Columns("ContactID","Name","Title","WorkEmail","JobfunctionID", "JobpositionID","StateID")
                .WithMetadata()
                .AutoJoin("AccountID")
                .From("Contact").WithMetadata().AutoJoin("JobfunctionID")
                .From("Contact").WithMetadata().AutoJoin("JobpositionID")
                .Where<decimal>("Contact", "ContactID", SqlOperators.Equal, 1429)
                .Builder();
            
            ResultTable result = builder.Execute();
            Form model = FormFactory.Default.BuildForm(builder);
            
            MetadataColumn mc;
            if (table.Columns.TryGetValue("JobfunctionID", out mc))
            {
                
            }
            

            model.Initialize(result.First());
            //model.FormLayout = FormLayouts.Horizontal;
            //model.CssFormLayout = "form-horizontal";
            //model.Sections[0].SectionLayout = SectionLayouts.VerticalTwoColumns;
            //return View("~/Views/TinySql/Details/Form.cshtml", model);
            return View(model);
        }

        public ActionResult Index()
        {
            return View();
        }

        public ActionResult About()
        {
            ViewBag.Message = "Your application description page.";

            return View();
        }

        public ActionResult Contact()
        {
            ViewBag.Message = "Your contact page.";

            return View();
        }
    }
}