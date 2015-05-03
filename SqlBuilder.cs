﻿using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Reflection;
using System.Runtime.Caching;
using System.Text;
using System.Threading.Tasks;
using System.Transactions;
using System.Xml;
using TinySql.Metadata;

namespace TinySql
{
    public class SqlBuilder
    {
        public static SqlBuilder Select(int? Top = null, bool Distinct = false)
        {
            return new SqlBuilder()
            {
                StatementType = StatementTypes.Select,
                Top = Top,
                Distinct = Distinct
            };
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

        private static string _DefaultConnection = null;

        public static string DefaultConnection
        {
            get { return _DefaultConnection; }
            set { _DefaultConnection = value; }
        }

        public string ConnectionString { get; set; }

        public object[] Format;

        public MetadataDatabase Metadata { get; set; }

        public TempTable SelectIntoTable { get; set; }

        private ConcurrentDictionary<string, SqlBuilder> _SubQueries = new ConcurrentDictionary<string, SqlBuilder>();
        public ConcurrentDictionary<string, SqlBuilder> SubQueries
        {
            get { return _SubQueries; }
            set { _SubQueries = value; }
        }
        public SqlBuilder AddSubQuery(string Name, SqlBuilder Builder)
        {
            Builder.ParentBuilder = this;
            return _SubQueries.AddOrUpdate(Name, Builder, (k, v) => { return Builder; });
        }

        public SqlBuilder ParentBuilder { get; set; }



        private List<OrderBy> _OrderByClause = new List<OrderBy>();

        public List<OrderBy> OrderByClause
        {
            get { return _OrderByClause; }
            set { _OrderByClause = value; }
        }



        public SqlBuilder()
        {
            Initialize(null, null);
        }
        public SqlBuilder(string ConnectionString)
        {
            Initialize(ConnectionString, null);
        }

        public SqlBuilder(string ConnectionString, CultureInfo Culture)
        {
            Initialize(ConnectionString, Culture);
        }


        private void Initialize(string connectionString, CultureInfo culture)
        {
            WhereConditions.Builder = this;
            Culture = culture;
            ConnectionString = connectionString;
        }

        private static CacheItemPolicy _CachePolicy = new CacheItemPolicy() { AbsoluteExpiration = MemoryCache.InfiniteAbsoluteExpiration, SlidingExpiration = MemoryCache.NoSlidingExpiration };
        public static CacheItemPolicy CachePolicy
        {
            get { return SqlBuilder._CachePolicy; }
            set { SqlBuilder._CachePolicy = value; }
        }


        public static bool CacheSqlBuilder(string Key, SqlBuilder Builder, CacheItemPolicy Policy = null)
        {
            CacheItem item = MemoryCache.Default.AddOrGetExisting(new CacheItem(Key, Builder), (Policy ?? CachePolicy));
            return item.Value == null;
        }
        public static SqlBuilder CacheSqlBuilder(string Key)
        {
            CacheItem item = MemoryCache.Default.GetCacheItem(Key);
            return item != null ? (SqlBuilder)item.Value : null;
        }


        public string ToSql(params object[] Format)
        {
            if (Format == null)
            {
                return ToSql();
            }

            return string.Format(ToSql(), Format);
        }

        public string ToSql()
        {
            switch (StatementType)
            {
                case StatementTypes.Select:
                    return SelectSql();
                case StatementTypes.Insert:
                    return InsertSql();
                case StatementTypes.Update:
                    return UpdateSql();
                case StatementTypes.Delete:
                    return DeleteSql();
                default:
                    throw new NotImplementedException();
            }
        }

        private string DeleteSql()
        {

            StringBuilder sb = new StringBuilder();
            Table BaseTable = this.Tables[0];

            sb.AppendFormat("DELETE  {0}\r\n", BaseTable.Alias);
            sb.AppendFormat("  FROM  {0}\r\n", BaseTable.ReferenceName);
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
            InsertIntoTable BaseTable = this.Tables[0] as InsertIntoTable;
            string declare = "";
            string set = "";
            string outputSelect = "";
            foreach (ParameterField field in BaseTable.FieldList)
            {
                declare += field.DeclareParameter() + "\r\n";
                set += field.SetParameter() + "\r\n";
            }

            if (BaseTable.Output != null)
            {
                declare += BaseTable.Output.DeclareParameter() + "\r\n";
                outputSelect += BaseTable.Output.SetParameter() + "\r\n";

            }

            sb.AppendLine(declare);
            sb.AppendLine(set);
            sb.AppendFormat(" INSERT  INTO {0}({1})\r\n", BaseTable.Alias, BaseTable.ToSql());
            if (BaseTable.Output != null)
            {
                sb.AppendFormat("OUTPUT  {0}\r\n", BaseTable.Output.ToSql());
            }
            sb.AppendFormat("VALUES({0})\r\n", BaseTable.FieldParameters());
            if (!string.IsNullOrEmpty(outputSelect))
            {
                sb.AppendLine(outputSelect);
            }
            return sb.ToString();

        }
        private string SelectSql()
        {
            StringBuilder sb = new StringBuilder();
            Table BaseTable = this.Tables[0];
            string selectList = BaseTable.ToSql();
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
            sb.AppendFormat("  FROM  {0}\r\n", BaseTable.ReferenceName);
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
            if (this.SelectIntoTable != null && this.SelectIntoTable.OutputTable)
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
            if (_SubQueries.Count > 0)
            {
                foreach (SqlBuilder sub in _SubQueries.Values)
                {
                    sb.AppendFormat("\r\n-- Sub Query\r\n{0}\r\n", sub.ToSql(Format));
                }
            }

            return sb.ToString();
        }

        private string UpdateSql()
        {
            StringBuilder sb = new StringBuilder();
            UpdateTable BaseTable = this.Tables[0] as UpdateTable;
            string declare = "";
            string set = "";
            string outputSelect = "";
            foreach (ParameterField field in BaseTable.FieldList.OfType<ParameterField>())
            {
                declare += field.DeclareParameter() + "\r\n";
                set += field.SetParameter() + "\r\n";
            }
            if (BaseTable.Output != null)
            {
                declare += BaseTable.Output.DeclareParameter() + "\r\n";
                outputSelect += BaseTable.Output.SetParameter() + "\r\n";

            }

            sb.AppendLine(declare);
            sb.AppendLine(set);

            sb.AppendFormat("UPDATE  {0}\r\n", BaseTable.Name);
            sb.AppendFormat("   SET  {0}\r\n", BaseTable.ToSql());
            if (BaseTable.Output != null)
            {
                sb.AppendFormat("OUTPUT  {0}\r\n", BaseTable.Output.ToSql());
            }
            sb.AppendFormat("  FROM  {0}\r\n", BaseTable.ReferenceName);
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
            Delete = 4
        }

        private System.Globalization.CultureInfo _Culture = null;

        public System.Globalization.CultureInfo Culture
        {
            get { return _Culture ?? DefaultCulture; }
            set { _Culture = value; }
        }
        public static CultureInfo DefaultCulture
        {
            get
            {
                return System.Globalization.CultureInfo.GetCultureInfo(1033);
            }
        }


        public List<Table> Tables = new List<Table>();
        public WhereConditionGroup WhereConditions = new WhereConditionGroup();



        public List<Join> JoinConditions = new List<Join>();
        public StatementTypes StatementType
        {
            get;
            private set;
        }

        public int? Top
        {
            get;
            private set;
        }
        public bool Distinct
        {
            get;
            private set;
        }
    }
}