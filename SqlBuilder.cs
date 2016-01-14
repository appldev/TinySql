using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Runtime.Caching;
using System.Text;
using TinySql.Metadata;

namespace TinySql
{
    public class SqlBuilder
    {
        public static SqlBuilder Select(int? top = null, bool distinct = false)
        {
            return new SqlBuilder()
            {
                StatementType = StatementTypes.Select,
                Top = top,
                Distinct = distinct
            };
        }
        public static StoredProcedure StoredProcedure(string name, string schema = null)
        {

            SqlBuilder builder = new SqlBuilder()
            {
                StatementType = StatementTypes.Procedure
            };
            builder.Procedure = new StoredProcedure()
            {
                Builder = builder,
                Name = name,
                Schema = schema
            };
            return builder.Procedure;

        }
        public static SqlBuilder Insert()
        {
            return new SqlBuilder()
            {
                StatementType = StatementTypes.Insert
            };
        }

        public static SqlBuilder Update()
        {
            return new SqlBuilder()
            {
                StatementType = StatementTypes.Update
            };
        }

        public static SqlBuilder Delete()
        {
            return new SqlBuilder()
            {
                StatementType = StatementTypes.Delete
            };
        }

        public static IfStatement If()
        {
            return new IfStatement()
            {
                BranchStatement = BranchStatements.If,
                StatementType = StatementTypes.If
            };
        }

        public StoredProcedure Procedure { get; set; }

        private static string _defaultConnection = null;

        public static string DefaultConnection
        {
            get { return _defaultConnection; }
            set { _defaultConnection = value; }
        }

        private string _connectionString = null;

        public string ConnectionString
        {
            get { return _connectionString ?? DefaultConnection; }
            set { _connectionString = value; }
        }

        public object[] Format;

        private MetadataDatabase _metadata = null;
        public MetadataDatabase Metadata
        {
            get
            {
                MetadataDatabase mdb = _metadata ?? this.Builder()._metadata;
                return mdb ?? DefaultMetadata;
            }
            set { _metadata = value; }
        }

        public static MetadataDatabase DefaultMetadata { get; set; }


        public TempTable SelectIntoTable { get; set; }

        private ConcurrentDictionary<string, SqlBuilder> _subQueries = new ConcurrentDictionary<string, SqlBuilder>();
        public ConcurrentDictionary<string, SqlBuilder> SubQueries
        {
            get { return _subQueries; }
            set { _subQueries = value; }
        }
        public SqlBuilder AddSubQuery(string name, SqlBuilder builder)
        {
            if (builder.StatementType != StatementTypes.Insert && builder.StatementType != StatementTypes.Update)
            {
                // Only set the parent builder for statements that share parameter declarations
                builder.ParentBuilder = this;
            }
            return _subQueries.AddOrUpdate(name, builder, (k, v) => { return builder; });

        }

        public SqlBuilder ParentBuilder { get; set; }

        private ConcurrentDictionary<string, string> _declarations = new ConcurrentDictionary<string, string>();

        public SqlBuilder TopBuilder
        {
            get
            {
                if (ParentBuilder == null)
                {
                    return this;
                }
                SqlBuilder sb = ParentBuilder;
                while (sb.ParentBuilder != null)
                {
                    sb = sb.ParentBuilder;
                }
                return sb;
            }
        }

        internal bool AddDeclaration(string declarationName, string body)
        {
            return _declarations.TryAdd(declarationName, body);
        }

        private List<OrderBy> _orderByClause = new List<OrderBy>();

        public List<OrderBy> OrderByClause
        {
            get { return _orderByClause; }
            set { _orderByClause = value; }
        }

        public string BuilderName { get; set; }

        public SqlBuilder()
        {
            Initialize(null, null);
        }
        public SqlBuilder(string connectionString)
        {
            Initialize(connectionString, null);
        }

        public SqlBuilder(string connectionString, CultureInfo culture)
        {
            Initialize(connectionString, culture);
        }


        private void Initialize(string connectionString, CultureInfo culture)
        {
            WhereConditions.Builder = this;
            Culture = culture;
            ConnectionString = connectionString;
        }

        private static CacheItemPolicy _cachePolicy = new CacheItemPolicy() { AbsoluteExpiration = ObjectCache.InfiniteAbsoluteExpiration, SlidingExpiration = ObjectCache.NoSlidingExpiration };
        public static CacheItemPolicy CachePolicy
        {
            get { return _cachePolicy; }
            set { _cachePolicy = value; }
        }


        public static bool CacheSqlBuilder(string key, SqlBuilder builder, CacheItemPolicy policy = null)
        {
            CacheItem item = MemoryCache.Default.AddOrGetExisting(new CacheItem(key, builder), (policy ?? CachePolicy));
            return item.Value == null;
        }
        public static SqlBuilder CacheSqlBuilder(string key)
        {
            CacheItem item = MemoryCache.Default.GetCacheItem(key);
            return item != null ? (SqlBuilder)item.Value : null;
        }


        public string ToSql(params object[] format)
        {
            if (format == null || format.Length == 0)
            {
                return ToSql();
            }

            return string.Format(ToSql(), format);
        }

        public virtual string ToSql()
        {
            StringBuilder sb = new StringBuilder();
            string sql = "";

            switch (StatementType)
            {
                case StatementTypes.Select:
                    sql = SelectSql();
                    break;
                case StatementTypes.Insert:
                    sql = InsertSql();
                    break;
                case StatementTypes.Update:
                    sql = UpdateSql();
                    break;
                case StatementTypes.Delete:
                    sql = DeleteSql();
                    break;
                case StatementTypes.Procedure:
                    sql = Procedure.ToSql();
                    break;
                default:
                    break;
            }

            // Post SQL
            if (ParentBuilder == null)
            {
                // Top level, so write parameter declarations at the top
                foreach (string par in _declarations.Values)
                {
                    sb.AppendLine(par);
                }
            }
            sb.AppendLine(sql);

            return sb.ToString();
        }

        private string DeleteSql()
        {

            StringBuilder sb = new StringBuilder();
            sb.AppendFormat("DELETE  {0}\r\n", BaseTable().Alias);
            sb.AppendFormat("  FROM  {0}\r\n", BaseTable().ReferenceName);
            foreach (Join j in JoinConditions)
            {
                sb.AppendFormat("{0}\r\n", j.ToSql());
            }
            string where = WhereConditions.ToSql();
            if (!string.IsNullOrEmpty(where))
            {
                sb.AppendFormat("WHERE {0}\r\n", where);
            }
            return sb.ToString();
        }
        private string InsertSql()
        {
            StringBuilder sb = new StringBuilder();
            InsertIntoTable baseTable = BaseTable() as InsertIntoTable;
            string set = "";
            string outputSelect = "";
            SqlBuilder tb = TopBuilder;
            foreach (ParameterField field in baseTable.FieldList)
            {
                tb.AddDeclaration(field.ParameterName, field.DeclareParameter());
                set += field.SetParameter() + "\r\n";
            }

            if (baseTable.Output != null && baseTable.Output.ParameterTable.FieldList.Count > 0)
            {
                tb.AddDeclaration(baseTable.Output.ParameterName, baseTable.Output.DeclareParameter());
                outputSelect += baseTable.Output.SetParameter() + "\r\n";

            }

            sb.AppendLine(set);
            sb.AppendFormat(" INSERT  INTO {0}({1})\r\n", baseTable.Alias, baseTable.ToSql());
            if (baseTable.Output != null && baseTable.Output.ParameterTable.FieldList.Count > 0)
            {
                sb.AppendFormat("OUTPUT  {0}\r\n", baseTable.Output.ToSql());
            }
            sb.AppendFormat("VALUES({0})\r\n", baseTable.FieldParameters());
            if (!string.IsNullOrEmpty(outputSelect))
            {
                sb.AppendLine(outputSelect);
            }
            return sb.ToString();

        }

        public void CleanSelectList(bool removeDublicateFields = false)
        {
            List<string> clean = new List<string>();
            int idx = 0;
            foreach (Field f in Tables.SelectMany(x => x.FieldList))
            {
                string s = f.Alias != null ? f.Alias : f.Name;
                if (!clean.Contains(s))
                {
                    clean.Add(s);
                }
                else
                {
                    if (removeDublicateFields)
                    {
                        f.Table.FieldList.Remove(f);
                    }
                    else
                    {
                        f.Alias = f.Table.Name + "_" + f.Name;
                        clean.Add(f.Alias);
                        idx++;
                    }
                    
                }
            }
        }

        private string SelectSql()
        {
            StringBuilder sb = new StringBuilder();

            // Clean select list
            CleanSelectList(false);


            string selectList = BaseTable().ToSql();
            foreach (Table t in Tables.Skip(1))
            {
                string fields = t.ToSql();
                selectList += !string.IsNullOrEmpty(fields) ? ", " + fields : "";
            }
            //
            // SELECT
            //
            sb.AppendFormat("SELECT {1} {2}  {0}\r\n", selectList, Distinct ? "DISTINCT" : "", Top.HasValue ? "TOP " + Top.Value.ToString() : "");
            //
            // INTO
            //
            if (SelectIntoTable != null)
            {
                sb.AppendFormat("  INTO  {0}\r\n", SelectIntoTable.ReferenceName);
            }
            //
            // FROM
            //
            sb.AppendFormat("  FROM  {0}\r\n", BaseTable().ReferenceName);
            //
            // JOINS
            //
            foreach (Join j in JoinConditions)
            {
                sb.AppendFormat("{0}\r\n", j.ToSql());
            }
            //
            // WHERE
            //
            string where = WhereConditions.ToSql();
            if (where != "()" && !string.IsNullOrEmpty(where))
            {
                sb.AppendFormat("WHERE {0}\r\n", where);
            }
            //
            // TODO: Group by
            //

            // ORDER BY
            if (OrderByClause.Count > 0)
            {
                sb.AppendFormat(" ORDER  BY {0}", OrderByClause.First().ToSql());
                foreach (OrderBy order in OrderByClause.Skip(1))
                {
                    sb.AppendFormat(", {0}", order.ToSql());
                }
                sb.Append("\r\n");
            }


            //
            // Post SQL stuff
            // 
            if (SelectIntoTable != null && SelectIntoTable.OutputTable)
            {
                sb.AppendFormat("SELECT  {0} FROM {1}\r\n", SelectIntoTable.ToSql(), SelectIntoTable.ReferenceName);
                if (SelectIntoTable.OrderByClause.Count > 0)
                {
                    sb.AppendFormat(" ORDER  BY {0}", SelectIntoTable.OrderByClause.First().ToSql());
                    foreach (OrderBy order in SelectIntoTable.OrderByClause.Skip(1))
                    {
                        sb.AppendFormat(", {0}", order.ToSql());
                    }
                    sb.Append("\r\n");
                }
            }

            //
            // Sub Queries
            //
            if (_subQueries.Count > 0)
            {
                foreach (SqlBuilder sub in _subQueries.Values)
                {
                    sb.AppendFormat("\r\n-- Sub Query\r\n{0}\r\n", sub.ToSql(Format));
                }
            }

            return sb.ToString();
        }

        private string UpdateSql()
        {
            StringBuilder sb = new StringBuilder();
            UpdateTable baseTable = BaseTable() as UpdateTable;
            string declare = "";
            string set = "";
            string outputSelect = "";
            SqlBuilder tb = TopBuilder;

            foreach (ParameterField field in baseTable.FieldList.OfType<ParameterField>())
            {
                tb.AddDeclaration(field.ParameterName, field.DeclareParameter());
                set += field.SetParameter() + "\r\n";
            }
            if (baseTable.Output != null && baseTable.Output.ParameterTable.FieldList.Count > 0)
            {
                tb.AddDeclaration(baseTable.Output.ParameterName, baseTable.Output.DeclareParameter());
                outputSelect += baseTable.Output.SetParameter() + "\r\n";

            }

            sb.AppendLine(declare);
            sb.AppendLine(set);

            sb.AppendFormat("UPDATE  {0}\r\n", baseTable.Name);
            sb.AppendFormat("   SET  {0}\r\n", baseTable.ToSql());
            if (baseTable.Output != null && baseTable.Output.ParameterTable.FieldList.Count > 0)
            {
                sb.AppendFormat("OUTPUT  {0}\r\n", baseTable.Output.ToSql());
            }
            sb.AppendFormat("  FROM  {0}\r\n", baseTable.ReferenceName);
            foreach (Join j in JoinConditions)
            {
                sb.AppendFormat("{0}\r\n", j.ToSql());
            }
            string where = WhereConditions.ToSql();
            if (!string.IsNullOrEmpty(where))
            {
                sb.AppendFormat("WHERE {0}\r\n", where);
            }
            if (!string.IsNullOrEmpty(outputSelect))
            {
                sb.AppendLine(outputSelect);
            }
            return sb.ToString();
        }

        public enum StatementTypes
        {
            Select = 1,
            Insert = 2,
            Update = 3,
            Delete = 4,
            If = 5,
            Procedure = 6
        }

        private CultureInfo _culture = null;

        public CultureInfo Culture
        {
            get { return _culture ?? DefaultCulture; }
            set { _culture = value; }
        }
        private static CultureInfo _defaultCulture = CultureInfo.GetCultureInfo(1033);
        public static CultureInfo DefaultCulture
        {
            get
            {
                return _defaultCulture;
            }
            set
            {
                _defaultCulture = value;
            }
        }


        public List<Table> Tables = new List<Table>();
        public WhereConditionGroup WhereConditions = new WhereConditionGroup();

        public virtual Table BaseTable()
        {
            return Tables.Count > 0 ? Tables[0] : null;
        }


        public List<Join> JoinConditions = new List<Join>();
        public StatementTypes StatementType
        {
            get;
            set;
        }

        public int? Top
        {
            get;
            set;
        }
        public bool Distinct
        {
            get;
            set;
        }
    }
}