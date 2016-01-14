using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

namespace TinySql.Metadata.Sql.CommandLine
{
    public delegate void MetadataUpdateDelegate(int PercentDone, string Message, DateTime timestamp);

    public static class ClassCreationOptions
    {
        public static List<string> Usings = new List<string>() { "System", "System.Collections.Generic", "TinySql.Attributes" };
        public static string Namespace = "TinySql.Classes";
        public static bool PartialClass = false;
        public static bool ColumnAsProperty = true;
        public static bool DecorateColumnAttributes = false;
        public static string OutputPath = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.CommonApplicationData), "TinySql");
        public static MetadataUpdateDelegate MetadataEvent = null;
        

    }
    public static class ClassGenerator
    {

        public static void CreateClasses(MetadataDatabase mdb)
        {
            if (!Directory.Exists(ClassCreationOptions.OutputPath))
            {
                Directory.CreateDirectory(ClassCreationOptions.OutputPath);
            }
            double total = mdb.Tables.Count;
            double done = 0;
            foreach (MetadataTable mt in mdb.Tables.Values)
            {
                StringBuilder sb = CreateTable(mt);
                string file = Path.Combine(ClassCreationOptions.OutputPath, mt.Name + ".cs");
                File.WriteAllText(file, sb.ToString());
                if (ClassCreationOptions.MetadataEvent != null)
                {
                    done++;
                    ClassCreationOptions.MetadataEvent.Invoke(Convert.ToInt32((done / total) * 100), mt.Name + ".cs created", DateTime.Now);
                }
            }
        }

        private static StringBuilder CreateTable(MetadataTable mt)
        {
            StringBuilder sb = new StringBuilder();
            foreach (string u in ClassCreationOptions.Usings)
            {
                sb.AppendFormat("using {0};\r\n", u);
            }
            sb.AppendLine("");
            sb.AppendFormat("namespace {0}\r\n{{", ClassCreationOptions.Namespace);

            
            sb.AppendFormat("\tpublic{0} class {1}\r\n{{\r\n", ClassCreationOptions.PartialClass ? " partial" : "", mt.Name);
            foreach (MetadataColumn mc in mt.Columns.Values.OrderBy(x => x.Name).OrderByDescending(x => x.IsPrimaryKey))
            {
                CreateColumn(mc, mt, sb);
            }
            sb.AppendLine("\t}\r\n}");
            return sb;
        }

        

        private static void CreateColumn(MetadataColumn mc, MetadataTable mt, StringBuilder sb)
        {
            string format = "\t\tpublic {0}{1}{2} {3}{4}\r\n\r\n";
            string FKformat = "\t\t[FK(\"{0}\",\"{1}\",\"{2}\",\"{3}\")]\r\n";

            if (mc.IsForeignKey)
            {
                List<MetadataForeignKey> FKs = new List<MetadataForeignKey>();
                try
                {
                    IEnumerable<MetadataForeignKey> fks = mt.FindForeignKeys(mc);
                    FKs.AddRange(fks);
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                
                if (FKs.Count == 0)
                {
                    throw new InvalidOperationException(string.Format("The foreign key for Column {0} in table {1} cannot be resolved to one key", mc.Name, mt.Fullname));
                }
                MetadataForeignKey FK = FKs.First();
                sb.AppendFormat(FKformat, FK.ReferencedTable,FK.ColumnReferences.First().ReferencedColumn.Name, FK.ReferencedSchema, FK.Name);
            }
            if (mc.IsPrimaryKey)
            {
                sb.AppendLine("\t\t[PK]");
            }
            bool nullable = mc.Nullable && (mc.DataType != typeof(string) && !mc.DataType.IsByRef && !mc.DataType.IsArray);
            if (ClassCreationOptions.ColumnAsProperty)
            {
                sb.AppendFormat(format, (nullable ? "Nullable<" : ""),mc.DataType.Name, nullable ? "> " : " ", mc.Name, " { get; set; }");
            }
            else
            {
                sb.AppendFormat(format, nullable ? "Nullable<" : "", mc.DataType.Name, nullable ? "> " : " ", mc.Name, ";");
            }
        }


    }
}
