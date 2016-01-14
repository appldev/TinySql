using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using TinySql.Metadata;

namespace TinySql
{

   

    
    
    public class MetadataHelper
    {
        internal MetadataTable Model { get; set; }
        internal Table Table { get; set; }
    }

    public class MetadataHelper<TClass> : MetadataHelper
    {
        internal TClass Class { get; set; }
        internal Type ClassType
        {
            get { return typeof(TClass); }
        }
        // internal Stack<Table> FromTables = new Stack<Table>();
        internal Stack<MetadataHelper> Helpers = new Stack<MetadataHelper>();
    }

    

    public static class MetadataExtensions
    {
        #region Initialize the helpers

        internal static string GetMetaTableName(Table t)
        {
            string s = t.FullName;
            return s.Contains('.') ? s : "dbo." + s;
        }

        internal static string GetMemberName<TClass,TProperty>(this MetadataHelper<TClass> helper, Expression<Func<TClass,TProperty>> property)
        {
            if (property.Body.NodeType == ExpressionType.MemberAccess)
            {
                return (property.Body as MemberExpression).Member.Name;
            }
            return null;
        }
        internal static string GetMemberName<TClass, TProperty>(Expression<Func<TClass, TProperty>> property)
        {
            if (property.Body.NodeType == ExpressionType.MemberAccess)
            {
                return (property.Body as MemberExpression).Member.Name;
            }
            return null;
        }

        public static MetadataHelper<TClass> WithMetadata<TClass>(this SqlBuilder builder, string tableName = null, string schema = null, string alias = null)
        {
            
                
            MetadataHelper<TClass> helper = new MetadataHelper<TClass>();
            helper.Model = builder.Metadata.FindTable(tableName == null ? helper.ClassType.Name : tableName);
            helper.Table = builder.From(helper.Model.Name, alias, helper.Model.Schema == "dbo" ? null : helper.Model.Schema);
            return helper;
        }

        public static MetadataHelper WithMetadata(this Table table)
        {
            MetadataDatabase mdb = table.Builder.Metadata;
            if (mdb != null)
            {
                MetadataTable mt = mdb.FindTable(table.FullName);
                if (mt != null)
                {
                    return new MetadataHelper() { Table = table, Model = mt };
                }
                else
                {
                    throw new InvalidOperationException(string.Format("The Table '{0}' was not found in metadata", table.FullName));
                }
            }
            else
            {
                throw new InvalidOperationException("The SqlBuilder does not contain metadata");
            }
        }

        public static SqlBuilder Builder(this MetadataHelper helper)
        {
            return helper.Table.Builder();
        }

        #endregion

        #region Metadata

        public static string[] TitleColumns = new string[] { "name", "title", "description", "fullname", "navn","titel","beskrivelse" };
        public static string GuessTitleColumn(this MetadataTable table)
        {
            if (!string.IsNullOrEmpty(table.TitleColumn))
            {
                return table.TitleColumn;
            }
            else
            {
            
                foreach (string s in TitleColumns)
                {
                    MetadataColumn mc = table.Columns.Values.FirstOrDefault(x => x.Name.IndexOf(s, StringComparison.OrdinalIgnoreCase) >= 0);
                    if (mc != null) { return mc.Name; }
                }
            }
            return table.PrimaryKey.Columns.First().Name;
        }

        public static SqlBuilder ToSqlBuilder(this MetadataColumn foreignKeyColumn)
        {
            MetadataForeignKey fk = foreignKeyColumn.Parent.FindForeignKeys(foreignKeyColumn).First();
            MetadataTable pk = SqlBuilder.DefaultMetadata.FindTable(fk.ReferencedSchema + "." + fk.ReferencedTable);
            string namecol = pk.GuessTitleColumn();
            string[] valuecols = fk.ColumnReferences.Where(x => !x.Column.IsComputed).Select(x => x.ReferencedColumn.Name).ToArray();

            SqlBuilder builder = SqlBuilder.Select()
                .From(pk.Name,null,pk.Schema)
                .Column(namecol)
                .Columns(valuecols)
                .Builder();
            List<MetadataColumnReference> mcrs = fk.ColumnReferences.Where(x => x.Column.IsComputed).ToList();
            if (mcrs.Count > 0)
            {
                MetadataColumnReference first = mcrs.First();
                builder.From(pk.Name, null, pk.Schema)
                    .Where(pk.Name, first.ReferencedColumn.Name, SqlOperators.Equal, (object)first.Column.Name.Trim('\"'))
                    .Builder();
                foreach (MetadataColumnReference mcr in mcrs.Skip(1))
                {
                    builder.From(pk.Name, null, pk.Schema)
                    .Where(pk.Name, mcr.ReferencedColumn.Name, SqlOperators.Equal, (object)mcr.Column.Name.Trim('\"'))
                    .Builder();
                }
            }
            

            return builder;
        }

        public static SqlBuilder ToSqlBuilder(this MetadataTable table, string listName)
        {
            List<string> columns = new List<string>(table.PrimaryKey.Columns.Select(x => x.Name));
            List<string> columnDef = null;
            List<MetadataColumn> mcs = new List<MetadataColumn>();
            if (table.ListDefinitions.TryGetValue(listName, out columnDef))
            {

                mcs = table.Columns.Values.Where(x => columnDef.Contains(x.Name)).ToList();
            }
            else
            {
                mcs = table.Columns.Values.Where(x => x.IsRowGuid == false && x.IsPrimaryKey == false).ToList();
            }
            columns.AddRange(mcs.Select(x => x.Name));

            SqlBuilder builder =  SqlBuilder.Select()
                .From(table.Name, null, table.Schema).Builder();

            foreach (MetadataColumn mc in mcs.Where(x => x.IsForeignKey))
            {
                builder.BaseTable().WithMetadata().AutoJoin(mc.Name);
            }

            builder.From(table.Name, null, table.Schema).Columns(columns.ToArray());
           

            
            return builder;


        }


        #endregion

        #region Columns

        public static MetadataHelper<TClass> AllColumns<TClass>(this MetadataHelper<TClass> helper)
        {
            PropertyInfo[] props = helper.ClassType.GetProperties();
            for (int i = 0; i < props.Length; i++)
            {
                PropertyInfo p = props[i];
                if (p.CanWrite)
                {
                    if (helper.Model.Columns.ContainsKey(p.Name))
                    {
                        helper.Table.Column(p.Name);
                    }
                }
            }
            return helper;
        }
        public static MetadataHelper<TClass> Column<TClass,TProperty>(this MetadataHelper<TClass> helper, Expression<Func<TClass,TProperty>> property, string mapColumn = null)
        {

            string col = helper.GetMemberName(property);
            string alias = null;
            if (mapColumn != null)
            {
                alias = col;
            }
            helper.Table.Column(mapColumn == null ? col : mapColumn, alias);
            return helper;
        }

        public static MetadataHelper<TClass> Column<TClass, TOther, TProperty>(this MetadataHelper<TClass> helper, Expression<Func<TClass, TProperty>> property, Expression<Func<TOther, TProperty>> includeFrom)
        {
            string col = helper.GetMemberName(property);
            string other = GetMemberName<TOther, TProperty>(includeFrom);
            string mapColumn = col.Equals(other) ? null : col;
            return helper.Column(property,mapColumn);
        }

        #endregion

        public static MetadataHelper<TClass> InnerJoin<TClass,TProperty>(this MetadataHelper<TClass> helper, Expression<Func<TClass,TProperty>> property)
        {
            Table to = InnerJoin(helper, helper.GetMemberName(property));
            MetadataTable model = to.Builder.Metadata.FindTable(to.Name);
            MetadataHelper<TClass> newHelper = new MetadataHelper<TClass>()
            {
                 Model = model,
                  Table = to
            };
            newHelper.Helpers.Push(helper);
            return newHelper;
        }

        public static MetadataHelper<T> ToTable<T,TClass>(this MetadataHelper<TClass> helper)
        {
            MetadataHelper<T> newHelper = new MetadataHelper<T>()
            {
                Helpers = helper.Helpers,
                Model = helper.Model
            };
            helper.Helpers.Push(helper);
            return newHelper;
        }

        

        public static MetadataHelper<TClass> From<TClass>(this MetadataHelper<TClass> helper, string tableName = null)
        {
            MetadataHelper previous = helper.Helpers.Pop();
            if (tableName != null)
            {
                while (previous.Table.Name != tableName && helper.Helpers.Count > 0)
                {
                    previous = helper.Helpers.Pop();
                }
            }
            return (MetadataHelper<TClass>)previous;
        }


        #region Conditions
        public static Table WherePrimaryKey(this MetadataHelper helper, object[] keys)
        {
            MetadataColumn key = helper.Model.PrimaryKey.Columns.First();

            if (helper.Table.Builder.WhereConditions.Conditions.Count == 0)
            {
                helper.Table.Where(helper.Model.Name,key.Name, SqlOperators.Equal,keys[0]);
            }
            for (int i = 1; i < helper.Model.PrimaryKey.Columns.Count; i++)
            {
                helper.Table.Builder.WhereConditions.And(helper.Model.Name, helper.Model.PrimaryKey.Columns[i].Name, SqlOperators.Equal, keys[i]);
            }
            return helper.Table;
        }

        #endregion


        #region Join statements

        public static Table AutoJoin(this MetadataHelper helper, string foreignKeyField)
        {
            MetadataColumn fromField = null;
            if (!helper.Model.Columns.TryGetValue(foreignKeyField, out fromField))
            {
                throw new ArgumentException("The Field " + foreignKeyField + " was not found", "FromField");
            }
            if (fromField.Nullable)
            {
                return LeftJoin(helper, foreignKeyField);
            }
            else
            {
                return InnerJoin(helper, foreignKeyField);
            }
        }
        public static Table InnerJoin(this MetadataHelper helper, string foreignKeyField)
        {
            return JoinInternal(helper, foreignKeyField, Join.JoinTypes.Inner);
        }

        public static Table LeftJoin(this MetadataHelper helper, string foreignKeyField)
        {
            return JoinInternal(helper, foreignKeyField, Join.JoinTypes.LeftOuter);
        }
        public static Table RightJoin(this MetadataHelper helper, string foreignKeyField)
        {
            return JoinInternal(helper, foreignKeyField, Join.JoinTypes.RightOuter);
        }
        public static Table CrossJoin(this MetadataHelper helper, string foreignKeyField)
        {
            return JoinInternal(helper, foreignKeyField, Join.JoinTypes.Cross);
        }

        public static Table InnerJoin(this MetadataHelper helper, string toTable, string toSchema = null)
        {
            return JoinInternal(helper, toTable, toSchema == null ? "dbo" : toSchema, Join.JoinTypes.Inner);
        }

        public static Table LeftJoin(this MetadataHelper helper, string toTable, string toSchema = null)
        {
            return JoinInternal(helper, toTable, toSchema == null ? "dbo" : toSchema, Join.JoinTypes.LeftOuter);
        }

        public static Table RightJoin(this MetadataHelper helper, string toTable, string toSchema = null)
        {
            return JoinInternal(helper, toTable, toSchema == null ? "dbo" : toSchema, Join.JoinTypes.RightOuter);
        }

        public static Table CrossJoin(this MetadataHelper helper, string toTable, string toSchema = null)
        {
            return JoinInternal(helper, toTable, toSchema == null ? "dbo" : toSchema, Join.JoinTypes.Cross);
        }

        private static Table JoinInternal(MetadataHelper helper, string totable, string toSchema, Join.JoinTypes joinType)
        {
            if (helper.Model.PrimaryKey.Columns.Count != 1)
            {
                throw new InvalidOperationException("Only tables with one primary key field is supported");
            }
            MetadataColumn fromField = helper.Model.PrimaryKey.Columns.First();
            MetadataTable mt = helper.Table.Builder.Metadata.FindTable(toSchema + "." + totable);
            JoinConditionGroup jcg = To(helper.Table.Builder, helper.Table, helper.Model, fromField, mt, joinType, false);
            return jcg.ToTable();
        }

        private static Table JoinInternal(MetadataHelper helper, string foreignKeyField, Join.JoinTypes joinType)
        {
            MetadataColumn fromField = null;
            if (!helper.Model.Columns.TryGetValue(foreignKeyField, out fromField))
            {
                throw new ArgumentException("The Field " + foreignKeyField + " was not found", "FromField");
            }
            List<MetadataForeignKey> fks = new List<MetadataForeignKey>(helper.Model.FindForeignKeys(fromField));
            if (fks.Count != 1)
            {
                throw new ArgumentException("The Field " + foreignKeyField + " points to more than one table", "FromField");
            }
            MetadataForeignKey fk = fks.First();
            string table = fk.ReferencedSchema + "." + fk.ReferencedTable;
            MetadataTable toTable = helper.Table.Builder.Metadata.FindTable(table);
            if (toTable == null)
            {
                throw new InvalidOperationException("The table '" + table + "' was not found in metadata");
            }
            JoinConditionGroup jcg = To(helper.Table.Builder, helper.Table, helper.Model, fromField, toTable, joinType, true);
            
            Table _table = jcg.ToTable();

            if (fromField.IncludeColumns != null)
            {
                foreach (string include in fromField.IncludeColumns)
                {
                    string iName = include;
                    string iAlias = null;
                    if (include.IndexOf('=')>0)
                    {
                        iName = include.Split('=')[0];
                        iAlias = include.Split('=')[1];
                    }
                    else
                    {
                        iAlias = fromField.Name + "_" + iName;
                    }
                    _table.Column(iName, iAlias);
                }
            }

            return _table;

        }

        private static JoinConditionGroup To(SqlBuilder builder, Table fromSqlTable, MetadataTable fromTable, MetadataColumn fromField, MetadataTable toTable, Join.JoinTypes joinType, bool preferForeignKeyOverPrimaryKey = true)
        {
            MetadataDatabase mdb = builder.Metadata;
            List<MetadataForeignKey> fks = null;
            MetadataForeignKey fk = null;
            Join j = null;
            MetadataColumnReference mcr = null;
            JoinConditionGroup jcg = null;

            if (fromField.IsPrimaryKey)
            {
                if (!fromField.IsForeignKey || !preferForeignKeyOverPrimaryKey)
                {
                    fks = toTable.ForeignKeys.Values.Where(x => x.ReferencedTable.Equals(fromTable.Name) && x.ReferencedSchema.Equals(fromTable.Schema) && x.ColumnReferences.Any(y => y.ReferencedColumn.Equals(fromField))).ToList();
                    if (fks.Count != 1)
                    {
                        throw new InvalidOperationException(string.Format("The column '{0}' is referenced by {1} keys in the table {2}. Expected 1. Make the join manually",
                            fromField.Name, fks.Count, toTable.Fullname));
                    }
                    fk = fks.First();
                    j = SqlStatementExtensions.MakeJoin(joinType, fromSqlTable, toTable.Name, null, toTable.Schema.Equals("dbo") ? null : toTable.Schema);

                    mcr = fk.ColumnReferences.First();
                    jcg = j.On(fromField.Name, SqlOperators.Equal, mcr.Name);
                    return jcg;
                }
            }
            if (fromField.IsForeignKey)
            {
                fks = new List<MetadataForeignKey>(fromTable.FindForeignKeys(fromField, toTable.Name));
                if (fks.Count != 1)
                {
                    throw new InvalidOperationException(string.Format("The column '{0}' resolves to {1} keys in the table {2}. Expected 1. Make the join manually",
                            fromField.Name, fks.Count, toTable.Fullname));
                }
                fk = fks.First();
                j = SqlStatementExtensions.MakeJoin(joinType, fromSqlTable, toTable.Name, null, toTable.Schema.Equals("dbo") ? null : toTable.Schema);
                mcr = fk.ColumnReferences.First();
                jcg = j.On(fromField.Name, SqlOperators.Equal, mcr.ReferencedColumn.Name);
                
                if (fk.ColumnReferences.Count > 1)
                {
                    foreach (MetadataColumnReference mcr2 in fk.ColumnReferences.Skip(1))
                    {
                        if (mcr2.Name.StartsWith("\""))
                        {
                            // its a value reference
                            // jcg.And(FK.ReferencedTable, mcr2.ReferencedColumn.Name, SqlOperators.Equal, mcr2.Name.Trim('\"'),null);

                            decimal d;
                            object o;
                            if (decimal.TryParse(mcr2.Name.Trim('\"'),out d))
                            {
                                o = d;
                            }
                            else
                            {
                                o = mcr2.Name.Trim('\"');
                            }

                            jcg.And(mcr2.ReferencedColumn.Name, SqlOperators.Equal,o, null);
                        }
                        else
                        {
                            jcg.And(mcr2.Column.Name, SqlOperators.Equal, mcr2.ReferencedColumn.Name);
                        }
                    }
                }
                return jcg;
            }
            throw new ArgumentException(string.Format("The Column '{0}' in the table '{1}' must be a foreign key or primary key", fromField.Name, fromTable.Fullname), "fromField");
        }

        #endregion






    }
}
