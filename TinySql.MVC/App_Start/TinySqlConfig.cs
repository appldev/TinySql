using System.Web;
using TinySql.Serialization;

namespace TinySql.MVC
{
    public static class TinySqlConfig
    {
        public static void Initialize()
        {
            HttpServerUtility u = HttpContext.Current.Server;
            SqlBuilder.DefaultMetadata = SerializationExtensions.FromFile(u.MapPath("~/Content/Metadata/TinyCrm.json"));
            SqlBuilder.DefaultConnection = System.Configuration.ConfigurationManager.ConnectionStrings["TinyCrm"].ConnectionString;
            SqlBuilder.DefaultCulture = System.Globalization.CultureInfo.GetCultureInfo(1030);
            
        }
    }
}