using Newtonsoft.Json;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Transactions;
using System.Xml;
using TinySql.Metadata;

namespace TinySql
{



    public class ResultTable : IList<RowData>
    {
        private List<RowData> _results = new List<RowData>();

        public ResultTable() { }
        public ResultTable(DataTable dt, DateHandlingEnum? dateHandling = null)
        {
            DateHandling = dateHandling;
            Initialize(dt);
        }

        public ResultTable(SqlBuilder builder, int timeoutSeconds = 60, bool withMetadata = true, DateHandlingEnum? dateHandling = null, string useHiearchyField = null, params object[] format)
        {
            DataSet ds = builder.DataSet(builder.ConnectionString, timeoutSeconds, format);
            Table bt = builder.BaseTable();
            if (bt != null)
            {
                Metadata = GetMetadataTable(builder.Metadata, bt.FullName);
            }

            WithMetadata = withMetadata;
            DateHandling = dateHandling;

            Initialize(ds.Tables[0]);
            ResultTable current = this;
            int currentTable = 0;
            if (builder.SubQueries.Count > 0 && Metadata == null)
            {
                throw new ArgumentException("The query contains sub-queries but no metadata has been specified. Use metadata to populate the sub-queries", "withMetadata");
            }
            foreach (var kv in builder.SubQueries)
            {
                currentTable++;
                DataTable dt = ds.Tables[currentTable];
                List<DataColumn> pk = new List<DataColumn>();
                //foreach (MetadataColumn Column in this.Metadata.PrimaryKey.Columns)
                //{
                //    DataColumn col = dt.Columns[Column.Name];
                //    pk.Add(dt.Columns[Column.Name]);
                //}
                //dt.PrimaryKey = pk.ToArray();
                foreach (RowData rd in this)
                {
                    // DataView dv = new DataView(dt);
                    DataView dv = dt.DefaultView;
                    List<object> _pk = new List<object>();
                    string sort = "";
                    foreach (MetadataColumn column in Metadata.PrimaryKey.Columns)
                    {
                        _pk.Add(rd.Column(column.Name));
                        sort += (!string.IsNullOrEmpty(sort) ? ", " : "") + column.Name + " ASC";
                    }
                    if (!string.IsNullOrEmpty(useHiearchyField))
                    {
                        sort = useHiearchyField + " ASC";
                    }
                    dv.Sort = sort;
                    DataRowView[] filteredRows = dv.FindRows(pk.ToArray());
                    SubTable(rd, kv.Value, filteredRows, ds, currentTable, Metadata.Name, WithMetadata, useHiearchyField);
                }

            }
        }

        private MetadataTable GetMetadataTable(MetadataDatabase mdb, string tableName)
        {
            if (mdb != null && !string.IsNullOrEmpty(tableName))
            {
                return mdb.FindTable(tableName);
            }
            return null;
        }
        private void SubTable(RowData parent, SqlBuilder builder, DataRowView[] rows, DataSet ds, int currentTable, string key, bool withMetadata, string useHierachyField = null)
        {
            ResultTable rt = new ResultTable();
            rt.WithMetadata = withMetadata;
            Table bt = builder.BaseTable();
            if (bt != null)
            {
                rt.Metadata = GetMetadataTable(builder.Metadata, bt.FullName);
            }



            string propName = builder.BuilderName ?? rt.Metadata.Name + "List";
            //if (!PropName.EndsWith("List"))
            //{
            //    PropName += "List";
            //}
            propName = propName.Replace(".", "").Replace(" ", "").Replace("-", "");

            rt.Initialize(rows, ds.Tables[currentTable]);
            if (!parent.Column<ResultTable>(propName, rt))
            {
                throw new InvalidOperationException("Unable to set the child Resulttable " + propName);
            }
            foreach (var kv in builder.SubQueries)
            {
                currentTable++;
                DataTable dt = ds.Tables[currentTable];
                List<DataColumn> pk = new List<DataColumn>();
                foreach (RowData rd in rt)
                {
                    DataView dv = new DataView(dt);
                    List<object> _pk = new List<object>();
                    string sort = "";
                    foreach (MetadataColumn column in rt.Metadata.PrimaryKey.Columns)
                    {
                        _pk.Add(rd.Column(column.Name));
                        sort += (!string.IsNullOrEmpty(sort) ? ", " : "") + column.Name + " ASC";
                    }
                    if (!string.IsNullOrEmpty(useHierachyField))
                    {
                        sort = useHierachyField + " ASC";
                    }
                    dv.Sort = sort;
                    DataRowView[] filteredRows = dv.FindRows(_pk.ToArray());
                    SubTable(rd, kv.Value, filteredRows, ds, currentTable, rt.Metadata.Name, withMetadata, useHierachyField);
                }
            }




        }


        public ResultTable(MetadataTable mt, DataTable dt)
        {
            Metadata = mt;
            Initialize(dt);
        }

        private void Initialize(DataRowView[] rows, DataTable dt)
        {
            foreach (DataRowView row in rows)
            {
                _results.Add(new RowData(this, row.Row, dt.Columns));
            }
        }

        private void Initialize(DataTable dt)
        {
            foreach (DataRow row in dt.Rows)
            {
                _results.Add(new RowData(this, row, dt.Columns));
            }
        }

        public string Name { get; set; }

        private MetadataTable _metadata = null;
        //[JsonIgnore]
        public MetadataTable Metadata
        {
            get { return _metadata; }
            set { _metadata = value; }
        }

        private bool _withMetadata = true;

        public bool WithMetadata
        {
            get { return _withMetadata; }
            set { _withMetadata = value; }
        }





        public int IndexOf(RowData item)
        {
            return _results.IndexOf(item);
        }

        public void Insert(int index, RowData item)
        {
            _results.Insert(index, item);
        }

        public void RemoveAt(int index)
        {
            _results.RemoveAt(index);
        }

        public RowData this[int index]
        {
            get
            {
                return _results[index];
            }
            set
            {
                _results[index] = value;
            }
        }

        public void Add(RowData item)
        {
            _results.Add(item);
        }

        public void Clear()
        {
            _results.Clear();
        }

        public bool Contains(RowData item)
        {
            return _results.Contains(item);
        }

        public void CopyTo(RowData[] array, int arrayIndex)
        {
            _results.CopyTo(array, arrayIndex);
        }

        public int Count
        {
            get { return _results.Count; }
        }

        public bool IsReadOnly
        {
            get { return false; }
        }

        public bool Remove(RowData item)
        {
            return _results.Remove(item);
        }



        public IEnumerator<RowData> GetEnumerator()
        {
            return _results.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        public enum DateHandlingEnum
        {
            None = 0,
            ConvertToString = 1,
            ConvertToDate = 2
        }

        public static DateHandlingEnum DefaultDateHandling = DateHandlingEnum.None;
        private DateHandlingEnum? _dateHandling = null;

        public DateHandlingEnum? DateHandling
        {
            get { return _dateHandling ?? DefaultDateHandling; }
            set { _dateHandling = value; }
        }
    }

    public class RowData : DynamicObject, ICloneable
    {
        public static RowData Create(MetadataTable table, bool cachePrimaryKey = false, object[] pk = null)
        {
            ConcurrentDictionary<string, object> values = new ConcurrentDictionary<string, object>();
            foreach (MetadataColumn mc in table.Columns.Values)
            {
                object v = null;
                if (!mc.Nullable && mc.DataType.IsValueType)
                {
                    v = Activator.CreateInstance(mc.DataType);
                }

                if (!values.TryAdd(
                    mc.Name,
                    v
                    ))
                {
                    throw new ArgumentException("The value from the column" + mc.Name + " could not be added to the row");
                }
            }
            RowData row = new RowData(values, new ConcurrentDictionary<string, object>());
            row.LoadMetadata(table, cachePrimaryKey);
            if (pk != null)
            {
                for (int i = 0; i < table.PrimaryKey.Columns.Count; i++)
                {
                    row.OriginalValues.AddOrUpdate(table.PrimaryKey.Columns[i].Name, pk[i], (k, v) => { return pk[i]; });
                }
            }
            return row;
        }

        public RowData(ResultTable parent, DataRow dr, DataColumnCollection columns)
        {
            //_OriginalValues = new ConcurrentDictionary<string, object>(16, Columns.Count);
            //_ChangedValues = new ConcurrentDictionary<string, object>();
            foreach (DataColumn col in columns)
            {
                _columns.Add(col.ColumnName);
                object o = dr.IsNull(col) ? null : dr[col];
                string key = col.ColumnName.Replace(" ", "_");
                if (o != null && parent.DateHandling != ResultTable.DateHandlingEnum.None && o is DateTime)
                {
                    if (parent.DateHandling == ResultTable.DateHandlingEnum.ConvertToString)
                    {
                        o = ((DateTime)o).ToString(SqlBuilder.DefaultCulture);
                    }
                    else if (parent.DateHandling == ResultTable.DateHandlingEnum.ConvertToDate)
                    {
                        o = ((DateTime)o).ToString("G", SqlBuilder.DefaultCulture);
                    }

                }
                _originalValues.TryAdd(key, o);
                //_OriginalValues[key] = o;
                //if (!_OriginalValues.TryAdd(key, o))
                //{
                //    throw new InvalidOperationException(string.Format("Unable to set the RowData value {0} for Column {1}", o, Col.ColumnName));
                //}
            }
            LoadMetadata(parent.Metadata, parent.WithMetadata);
        }


        public RowData()
        {

        }

        public bool LoadMetadata()
        {
            if (Metadata != null)
            {
                return false;
            }
            string table = Table;
            if (string.IsNullOrEmpty(table))
            {
                return false;
            }
            MetadataTable mt = SqlBuilder.DefaultMetadata.FindTable(Table);
            if (mt == null)
            {
                return false;
            }
            LoadMetadata(mt, true);
            _columns = new List<string>(_originalValues.Keys);
            return true;

        }


        public void LoadMetadata(MetadataTable mt, bool cachePrimaryKey = false)
        {
            if (mt == null) { return; }
            _originalValues.AddOrUpdate("__TABLE", mt.Fullname, (k, v) => { return mt.Fullname; });
            
            if (cachePrimaryKey && mt.PrimaryKey!= null)
            {
                _originalValues.AddOrUpdate("__PK", mt.PrimaryKey.Columns, (k, v) => { return mt.PrimaryKey.Columns; });
            }
        }

        public string Table
        {
            get
            {
                if (!_originalValues.ContainsKey("__TABLE"))
                {
                    return null;
                }
                return Convert.ToString(_originalValues["__TABLE"]);
            }
        }

        private List<MetadataColumn> InternalPk
        {
            get
            {
                if (!_originalValues.ContainsKey("__PK"))
                {
                    return null;
                }
                else
                {
                    return (List<MetadataColumn>)_originalValues["__PK"];
                }
            }
        }


        [JsonIgnore]
        public MetadataTable Metadata
        {
            get
            {
                List<MetadataColumn> pk = InternalPk;
                if (pk != null)
                {
                    return pk.First().Parent;
                }
                return null;
            }
        }


        public WhereConditionGroup PrimaryKey(SqlBuilder builder)
        {
            List<MetadataColumn> columns = InternalPk;
            string table = Table;
            if (columns == null || table == null)
            {
                return null;
            }

            WhereConditionGroup pk = new WhereConditionGroup();
            pk.Builder = builder;
            foreach (MetadataColumn c in columns)
            {
                object o = null;
                if (InternalGet(c.Name, out o))
                {
                    pk.And(table, c.Name, SqlOperators.Equal, o);
                }
            }
            return pk;
        }

        internal RowData(ConcurrentDictionary<string, object> originalValues, ConcurrentDictionary<string, object> changedValues)
        {
            _originalValues = originalValues;
            _changedValues = changedValues;
        }


        private ConcurrentDictionary<string, object> _originalValues = new ConcurrentDictionary<string, object>();
        public ConcurrentDictionary<string, object> OriginalValues
        {
            get { return _originalValues; }
            set { _originalValues = value; }
        }
        private ConcurrentDictionary<string, object> _changedValues = new ConcurrentDictionary<string, object>();
        public ConcurrentDictionary<string, object> ChangedValues
        {
            get { return _changedValues; }
            set { _changedValues = value; }
        }

        public bool HasChanges
        {
            get { return _changedValues.Count > 0; }
        }

        public object Column(string name)
        {
            object o;
            if (InternalGet(name, out o))
            {
                return o;
            }
            else
            {
                throw new ArgumentException("The Column name '" + name + "' does not exist", "name");
            }
        }

        public bool Column(string name, object value)
        {
            return InternalSet(name, value);
        }
        public bool Column<T>(string name, T value)
        {
            return InternalSet(name, value);
        }

        public T Column<T>(string name)
        {
            object o = Column(name);
            return (T)o;
        }

        private List<string> _columns = new List<string>();

        public List<string> Columns
        {
            get { return _columns; }
            set { _columns = value; }
        }

        
        //public string[] Columns
        
        //    get
        //    {
        //        return Columns.ToArray();
        //        List<string> keys = new List<string>();
        //        //for (int i = 0; i < _OriginalValues.Keys.Count; i++)
        //        //{
        //        //    if (!_OriginalValues.Keys[i].StartsWith("__"))
        //        //    {
        //        //        keys.Add(_OriginalValues.Keys[i]);
        //        //    }
        //        //}
        //        //return keys.ToArray();
        //        foreach (string key in _OriginalValues.Keys)
        //        {
        //            if (!key.StartsWith("__"))
        //            {
        //                keys.Add(key);
        //            }
        //        }
        //        return keys.ToArray();
        //    }
        //}



        public void AcceptChanges()
        {
            using (TransactionScope trans = new TransactionScope(TransactionScopeOption.RequiresNew))
            {
                foreach (string key in _changedValues.Keys)
                {
                    object o;
                    if (_changedValues.TryRemove(key, out o))
                    {
                        _originalValues.AddOrUpdate(key, o, (k, v) => { return o; });
                    }
                    else
                    {
                        throw new InvalidOperationException(string.Format("Unable to get the value from the property '{0}'", key));
                    }
                }
                if (_changedValues.Count > 0)
                {
                    throw new InvalidOperationException(string.Format("There are still {0} unaccepted values", _changedValues.Count));
                }
                trans.Complete();
            }
        }




        public override IEnumerable<string> GetDynamicMemberNames()
        {
            return _originalValues.Keys.AsEnumerable();
            //foreach (string key in _OriginalValues.Keys)
            //{
            //    yield return key;
            //}
            //yield return "Parent";
        }

        public override bool TryGetMember(GetMemberBinder binder, out object result)
        {
            return InternalGet(binder.Name, out result);
        }

        public override bool TrySetMember(SetMemberBinder binder, object value)
        {
            return InternalSet(binder.Name, value);
        }

        private bool InternalSet(string column, object value)
        {
            object o;
            if (!OriginalValues.ContainsKey(column))
            {
                return _originalValues.TryAdd(column, value);
            }
            if (!_originalValues.TryGetValue(column, out o))
            {
                return false;

            }
            if ((o == null && value != null) || !o.Equals(value))
            {
                _changedValues.AddOrUpdate(column, value, (key, existing) =>
                {
                    return value;
                });
            }
            return true;
        }

        private bool InternalGet(string column, out object value)
        {
            if (_changedValues.TryGetValue(column, out value))
            {
                return true;
            }
            return _originalValues.TryGetValue(column, out value);
        }



        public object Clone()
        {
            return new RowData(_originalValues, _changedValues);
        }
    }

    public enum SqlOperators
    {
        // General
        Null,
        NotNull,
        // Math
        Equal,
        NotEqual,
        GreaterThan,
        GreaterThanEqual,
        LessThan,
        LessThanEqual,
        // String
        StartsWith,
        EndsWith,
        Contains,
        // Lists
        In,
        NotIn
    }

    public enum OrderByDirections
    {
        Asc,
        Desc
    }

    public enum BoolOperators
    {
        And,
        Or,
        None
    }

    public enum BranchStatements
    {
        If,
        ElseIf,
        Else
    }

    public class OrderBy
    {
        public Field Field { get; set; }
        public OrderByDirections Direction { get; set; }

        public string ToSql()
        {
            // return (Field.Table.Schema != null ? Field.Table.Schema + "." : "") + Field.Table.Name + ".[" + Field.Name + "] " + Direction.ToString().ToUpper();
            return Field.Table.Alias + ".[" + Field.Name + "] " + Direction.ToString().ToUpper();
        }
    }


    public class StoredProcedure
    {
        public SqlBuilder Builder { get; set; }

        private List<ParameterField> _parameters = new List<ParameterField>();
        public string Name { get; set; }


        public string Schema { get; set; }
        public string ReferenceName
        {
            get
            {
                return string.IsNullOrEmpty(Schema) ? Name : Schema + "." + Name;
            }
        }

        public List<ParameterField> Parameters
        {
            get { return _parameters; }
            set { _parameters = value; }
        }

        public string ToSql()
        {
            SqlBuilder tb = Builder.Builder();
            StringBuilder sql = new StringBuilder();
            string set = "";
            string call = "";
            foreach (ParameterField field in Parameters)
            {
                tb.AddDeclaration(field.ParameterName, field.DeclareParameter());
                set += field.SetParameter() + "\r\n";
                call += (!string.IsNullOrEmpty(call) ? ", " : "") + field.ParameterName + (field.IsOutput ? " OUT" : "");
            }

            sql.AppendLine("-- Stored procedure");
            sql.AppendFormat("{0}\r\n", set);
            sql.AppendFormat("EXEC {0} {1}\r\n", ReferenceName, call);
            string output = "";
            foreach (ParameterField field in Parameters.Where(x => x.IsOutput == true))
            {
                output += string.IsNullOrEmpty(output) ? field.ParameterName : ", " + field.ParameterName;
            }
            if (!string.IsNullOrEmpty(output))
            {
                sql.AppendFormat("SELECT  {0}\r\n", output);
            }
            return sql.ToString();
        }
    }

    public class IfStatement : SqlBuilder
    {
        public IfStatement()
        {
            StatementBody = new SqlBuilder();
            BranchStatement = BranchStatements.If;
            ElseIfStatements = new List<IfStatement>();
            _conditions.Builder = this;
            _conditions.Parent = this;
            StatementBody.ParentBuilder = this;
        }

        private SqlBuilder _builder = null;

        public SqlBuilder Builder
        {
            get { return _builder; }
            set
            {
                _builder = value;
            }
        }


        public SqlBuilder StatementBody { get; set; }

        public BranchStatements BranchStatement { get; set; }

        private IfElseConditionGroup _conditions = new IfElseConditionGroup();

        public IfElseConditionGroup Conditions
        {
            get { return _conditions; }
            set { _conditions = value; }
        }

        public List<IfStatement> ElseIfStatements { get; set; }

        public override Table BaseTable()
        {
            return StatementBody.BaseTable();
        }
        public override string ToSql()
        {
            StringBuilder sql = new StringBuilder();
            if (BranchStatement == BranchStatements.Else)
            {
                sql.AppendFormat("ELSE\r\nBEGIN\r\n");
            }
            else
            {
                sql.AppendFormat("{0} {1}\r\n", BranchStatement == BranchStatements.ElseIf ? "ELSE IF " : BranchStatement.ToString().ToUpper() + " ", _conditions.ToSql());
                sql.AppendLine("BEGIN");
            }

            sql.AppendFormat("{0}\r\n", StatementBody.ToSql());
            sql.AppendLine("END");
            foreach (IfStatement statement in ElseIfStatements)
            {
                sql.AppendLine(statement.ToSql());
            }
            return base.ToSql() + "\r\n" + sql.ToString();
        }



    }

    public class IfElseConditionGroup : WhereConditionGroup
    {
        public new IfStatement Parent { get; set; }
    }



    public class Join
    {
        private static string JoinClause(JoinTypes joinType)
        {
            switch (joinType)
            {
                case JoinTypes.Inner:
                    return "INNER JOIN";
                case JoinTypes.LeftOuter:
                    return "LEFT OUTER JOIN";
                case JoinTypes.RightOuter:
                    return "RIGHT OUTER JOIN";
                case JoinTypes.Cross:
                    return "CROSS JOIN";
                default:
                    return "";
            }
        }
        public Join()
        {
            Conditions.Join = this;
            Conditions.Builder = Builder;
        }
        public enum JoinTypes
        {
            Inner,
            LeftOuter,
            RightOuter,
            Cross
        }
        public JoinTypes JoinType { get; set; }
        public JoinConditionGroup Conditions = new JoinConditionGroup();
        public Table FromTable;
        public Table ToTable;
        private SqlBuilder _builder = null;

        public SqlBuilder Builder
        {
            get { return _builder; }
            set
            {
                _builder = value;
                Conditions.Builder = value;
            }
        }


        public string ToSql()
        {
            if (JoinType != JoinTypes.Cross)
            {
                return string.Format("{0} {1} ON {2}", JoinClause(JoinType), ToTable.ReferenceName, Conditions.ToSql());
            }
            else
            {
                return string.Format("{0} {1}", JoinClause(JoinType), ToTable.ReferenceName);
            }

        }

    }

    public class PrimaryKey : ConditionGroup
    {
        public PrimaryKey() { }
        public PrimaryKey(SqlBuilder builder, Table parent)
        {
            Builder = builder;
            Parent = parent;
        }

        public new Table Parent;
        // private new BoolOperators ConditionLink = BoolOperators.None;
        private List<ConditionGroup> _subConditions = new List<ConditionGroup>();
        public new List<FieldCondition> Conditions = new List<FieldCondition>();
        // public SqlBuilder Builder;
    }

    public class ConditionGroup
    {
        public BoolOperators ConditionLink = BoolOperators.None;
        public List<FieldCondition> Conditions = new List<FieldCondition>();
        public List<ConditionGroup> SubConditions = new List<ConditionGroup>();
        public ConditionGroup Parent;
        public SqlBuilder Builder;

        public virtual string ToSql()
        {
            if (Conditions.Count == 0 && SubConditions.Count == 0)
            {
                return "";
            }
            string sql = ConditionLink != BoolOperators.None ? " " + ConditionLink.ToString().ToUpper() + " (" : "(";
            foreach (FieldCondition condition in Conditions)
            {
                sql += condition.ToSql();
            }
            foreach (ConditionGroup group in SubConditions)
            {
                sql += group.ToSql();
            }
            sql += ")";
            return sql;
        }

    }

    public class WhereConditionGroup : ConditionGroup
    {

    }
    public class JoinConditionGroup : ConditionGroup
    {
        public Join Join;
    }

    public class ExistsConditionGroup : ConditionGroup
    {
        public Table InTable;
        public string FromTable;
        public bool Negated = true;
        public ExistsConditionGroup()
        {
            ConditionLink = BoolOperators.None;
        }

        public override string ToSql()
        {
            // (NOT) EXISTS (SELECT 1 FROM InTable WHERE (Con )
            string sql = ConditionLink == BoolOperators.None ? "" : " " + ConditionLink.ToString().ToUpper() + " ";
            sql += Negated ? "NOT EXISTS(SELECT 1 FROM {0} WHERE {1})" : "EXISTS(SELECT 1 FROM {0} WHERE {1})";
            BoolOperators op = ConditionLink;
            ConditionLink = BoolOperators.None;
            sql = string.Format(sql, InTable.Alias, base.ToSql());
            ConditionLink = op;
            return sql;

        }
    }


    public class FieldCondition
    {
        private static string GetOperator(SqlOperators Operator)
        {
            switch (Operator)
            {
                case SqlOperators.Null:
                    return "IS";
                case SqlOperators.NotNull:
                    return "IS NOT";
                case SqlOperators.Equal:
                    return "=";
                case SqlOperators.NotEqual:
                    return "!=";
                case SqlOperators.GreaterThan:
                    return ">";
                case SqlOperators.GreaterThanEqual:
                    return ">=";
                case SqlOperators.LessThan:
                    return "<";
                case SqlOperators.LessThanEqual:
                    return "<=";
                case SqlOperators.StartsWith:
                case SqlOperators.EndsWith:
                case SqlOperators.Contains:
                    return "LIKE";
                case SqlOperators.In:
                    return "IN";
                case SqlOperators.NotIn:
                    return "NOT IN";
                default:
                    return "";
            }
        }
        public BoolOperators ConditionLink = BoolOperators.None;
        public Table LeftTable;
        public Field LeftField;
        public Table RightTable;
        public Field RightField;
        public SqlOperators Condition;
        public SqlBuilder Builder;
        public ConditionGroup ParentGroup;
        public List<ConditionGroup> SubConditions = new List<ConditionGroup>();

        public virtual string ToSql()
        {
            string sql = ConditionLink != BoolOperators.None ? " " + ConditionLink.ToString().ToUpper() + " " : "";
            if (Condition == SqlOperators.NotNull || Condition == SqlOperators.Null)
            {
                sql += string.Format("{0} {1} NULL", LeftField.DeclarationName, GetOperator(Condition));
            }
            else if (Condition == SqlOperators.In || Condition == SqlOperators.NotIn)
            {
                sql += string.Format("{0} {1} ({2})", LeftField.DeclarationName, GetOperator(Condition), LeftField.ToSql());
            }
            else
            {
                if (RightField == null)
                {
                    string q = ((ValueField)LeftField).Quotable;
                    string qs = q == "'" ? "N'" : "";
                    switch (Condition)
                    {
                        case SqlOperators.Equal:
                        case SqlOperators.NotEqual:
                        case SqlOperators.GreaterThan:
                        case SqlOperators.GreaterThanEqual:
                        case SqlOperators.LessThan:
                        case SqlOperators.LessThanEqual:
                            sql += string.Format("{0} {1} {3}{2}{4}", LeftField.DeclarationName, GetOperator(Condition), LeftField.ToSql(), qs, q);
                            break;
                        case SqlOperators.StartsWith:
                            sql += string.Format("{0} {1} '{2}%'", LeftField.DeclarationName, GetOperator(Condition), LeftField.ToSql());
                            break;
                        case SqlOperators.EndsWith:
                            sql += string.Format("{0} {1} '%{2}'", LeftField.DeclarationName, GetOperator(Condition), LeftField.ToSql());
                            break;
                        case SqlOperators.Contains:
                            sql += string.Format("{0} {1} '%{2}%'", LeftField.DeclarationName, GetOperator(Condition), LeftField.ToSql());
                            break;
                        default:
                            break;
                    }
                }
                else
                {
                    sql += string.Format("{0} {1} {2}", LeftField.DeclarationName, GetOperator(Condition), RightField.ReferenceName);
                }
            }
            foreach (ConditionGroup group in SubConditions)
            {
                sql += group.ToSql();
            }
            return sql;
        }

    }



    public class TableParameterField : ParameterField
    {
        public TableParameterField()
        {
            ParameterTable = new Table();
        }
        //private new System.Data.SqlDbType SqlDataType;
        //private new int MaxLength = -1;
        //private new int Scale = -1;
        //private new bool IsOutput = false;
        public Table ParameterTable;
        public override string DeclareParameter()
        {
            string sql = string.Format("DECLARE {0} TABLE(", ParameterName);
            Field f = ParameterTable.FieldList.First();
            sql += f.Name + " " + f.GetSqlDataType();
            foreach (Field column in ParameterTable.FieldList.Skip(1))
            {
                sql += "," + column.Name + " " + column.GetSqlDataType();
            }
            sql += ")";
            return sql;
        }

        public override string SetParameter()
        {
            return "SELECT  * FROM " + ParameterName;
        }

        public override string ToSql()
        {
            string sql = string.Format("{0} INTO {1} \r\n", ParameterTable.ToSql(), ParameterName);
            return sql;
        }
    }


    public class ParameterField : ValueField
    {
        public string ParameterName { get; set; }
        private bool _isOutput = false;

        public bool IsOutput
        {
            get { return _isOutput; }
            set { _isOutput = value; }
        }

        public virtual string DeclareParameter()
        {
            string sql = string.Format("DECLARE {0} {1}", ParameterName, GetSqlDataType());
            //if (IsOutput)
            //{
            //    sql += " OUT";
            //}
            return sql;
        }



        public virtual string SetParameter()
        {
            if (Value == null || Value == DBNull.Value)
            {
                return string.Format("SET  {0} = NULL", ParameterName);
            }
            string q = GetQuotable(DataType);
            string qs = q == "'" ? "N'" : "";
            return string.Format("SET {0} = {2}{1}{3}", ParameterName, ToSql(), qs, q);
        }

        public override string ReferenceName
        {
            get
            {
                return Alias ?? Name;
            }
        }

    }



    public class ValueField : Field
    {
        // public new object Value;
        public Type DataType;

        public virtual string Quotable
        {
            get
            {
                return GetQuotable(Value.GetType());
            }
        }

        public override string ToSql()
        {
            string sql = null;
            object o = GetFieldValue(DataType == null ? Value.GetType() : DataType, Value, Builder.Culture);
            if (o == null)
            {
                sql = "NULL";
            }
            else
            {
                sql = Convert.ToString(o);
            }
            if (string.IsNullOrEmpty(Alias))
            {
                return sql;
            }
            else
            {
                string q = Quotable;
                sql = string.Format("{0}{1}{0} [{2}]", q, sql, Alias);
                return q.Length == 1 ? "N" + sql : sql;
            }
        }

    }

    public class ParameterField<T> : ParameterField
    {
        public ParameterField()
        {
            DataType = typeof(T);
        }

        public T FieldValue
        {
            get
            {
                return (T)Value;
            }
            set
            {
                Value = value;
            }
        }




    }

    public class ValueField<T> : ValueField
    {

        public override string Quotable
        {
            get
            {
                return GetQuotable(DataType);
            }
        }

        public ValueField()
        {
            DataType = typeof(T);
        }

        public T FieldValue
        {
            get
            {
                return (T)Value;
            }
            set
            {
                Value = value;
            }
        }



    }

    public class FunctionField : Field
    {
        //protected List<FieldBase> _Parameters = new List<FieldBase>();
        public List<FieldBase> Parameters = new List<FieldBase>();
        public string Schema { get; set; }
        public override string ToSql()
        {
            string sql = (!string.IsNullOrEmpty(Schema) ? Schema + "." : "") + Name + "(";
            if (Parameters.Count > 0)
            {
                sql += Parameters.First().ToSql();
            }
            foreach (FieldBase field in Parameters.Skip(1))
            {
                sql += ", " + field.ToSql();
            }
            sql += ")";
            if (!string.IsNullOrEmpty(Alias))
            {
                sql += " [" + Alias + "]";
            }
            return sql;
        }


    }

    public class BuiltinFn
    {
        private BuiltinFn()
        {

        }
        internal SqlBuilder Builder;
        internal Table Table;

        public BuiltinFn GetDate(string alias = null)
        {
            Table.FieldList.Add(
            new FunctionField()
            {
                Name = "GETDATE",
                Schema = null,
                Builder = Builder,
                Table = Table,
                Alias = alias
            });
            return this;
        }

        public enum AggregateTypes
        {
            Sum = 1,
            Max = 2,
            Min = 3
        }
        public BuiltinFn Aggregate(AggregateTypes aggregateType, string columnOrAlias, string alias = null)
        {

            return this;




        }


        public BuiltinFn Concat(string alias = null, params FieldBase[] values)
        {
            FunctionField fn = new FunctionField()
            {
                Name = "CONCAT",
                Schema = null,
                Builder = Builder,
                Table = Table,
                Alias = alias
            };
            fn.Parameters.AddRange(values);
            Table.FieldList.Add(fn);
            return this;
        }



        internal static BuiltinFn Fn(SqlBuilder builder, Table table)
        {
            BuiltinFn fn = new BuiltinFn();
            fn.Builder = builder;
            fn.Table = table;
            return fn;
        }

    }





    public class ConstantField<T> : FunctionField
    {
        public static ConstantField<T> Constant(T value)
        {
            ConstantField<T> c = new ConstantField<T>();
            c.Value = value;
            return c;
        }
        public ConstantField()
        {
            Name = null;
            Schema = null;
        }
        public new T Value { get; set; }
        public override string ToSql()
        {
            string q = GetQuotable(typeof(T));
            string sql = string.Format("{0}{1}{0}", q, GetFieldValue(typeof(T), Value));
            return q.Length == 1 ? "N" + sql : sql;
        }

    }

    public abstract class FieldBase
    {
        public string Name { get; set; }
        public abstract string ToSql();
    }


    public class Field : FieldBase
    {
        public Field()
        {

        }

        public string Alias
        {
            get;
            set;
        }

        public virtual object Value
        {
            get;
            set;
        }
        public Table Table { get; set; }
        public SqlBuilder Builder { get; set; }

        public SqlDbType SqlDataType;
        public int MaxLength = -1;
        private int _scale = -1;
        public int Scale
        {
            get { return _scale; }
            set { _scale = value; }
        }
        private int _precision = -1;
        public int Precision
        {
            get { return _precision; }
            set { _precision = value; }
        }
        public virtual string DeclarationName
        {
            get
            {
                string table = Table.Alias;
                return table + ".[" + Name + "]";
            }
        }
        public virtual string ReferenceName
        {
            get
            {
                string table = Table.Alias;
                return table + ".[" + (Alias ?? Name) + "]";
            }
        }

        public virtual string OutputName
        {
            get
            {
                return string.IsNullOrEmpty(Alias) ? Alias : Name;
            }
        }


        private bool IsDateData(SqlDbType sqlDataType)
        {
            return sqlDataType == SqlDbType.DateTime || sqlDataType == SqlDbType.DateTime2 || sqlDataType == SqlDbType.DateTimeOffset;
        }

        public string GetSqlDataType()
        {
            string sql = SqlDataType.ToString();
            //if (MaxLength != -1 && (SqlDataType != SqlDbType.DateTime || SqlDataType != SqlDbType.DateTime2 || SqlDataType != SqlDbType.DateTimeOffset))
            //{
            //    sql += "(" + (MaxLength == 0 ? "max" : MaxLength.ToString()) + (Scale != -1 ? "," + Scale : "") + ")";
            //}
            //else
            //{
            if (SqlDataType == SqlDbType.NVarChar || SqlDataType == SqlDbType.VarChar || SqlDataType == SqlDbType.Text || SqlDataType == SqlDbType.NText)
            {
                if (MaxLength <= 0)
                {
                    sql += "(MAX)";
                }
                else
                {
                    sql += string.Format("({0})", MaxLength);
                }
            }
            else if (SqlDataType == SqlDbType.Char || SqlDataType == SqlDbType.NChar)
            {
                if (MaxLength > 0)
                {
                    sql += string.Format("({0})", MaxLength);
                }
            }


            //else if (SqlDataType == SqlDbType.Xml)
            //{
            //    if (Value.GetType() == typeof(XmlDocument))
            //    {
            //        sql += string.Format("({0})", MaxLength);
            //    }
            //}
            else if (SqlDataType == SqlDbType.VarBinary)
            {
                if (Value != null)
                {

                    sql += "(" + ((byte[])Value).Length.ToString() + ")";
                }
            }
            else
            {
                if (Precision > 0 && !IsDateData(SqlDataType))
                {
                    sql += string.Format("({0}{1})", Precision, Scale >= 0 ? ", " + Scale.ToString() : "");
                }
            }
            //}
            return sql;
        }

        protected static string GetQuotable(Type dataType)
        {
            if (dataType == typeof(XmlDocument) || dataType == typeof(Guid) || dataType == typeof(string) || dataType == typeof(DateTime) || dataType == typeof(DateTimeOffset) || dataType == typeof(Guid?) || dataType == typeof(DateTime?) || dataType == typeof(DateTimeOffset?))
            {
                return "'";
            }
            else
            {
                return "";
            }
        }


        public static object GetFieldValue(Type dataType, object fieldValue, CultureInfo culture = null)
        {
            if (culture == null)
            {
                culture = SqlBuilder.DefaultCulture;
            }
            string fv = "";
            if ((dataType == typeof(Nullable<>) || dataType == typeof(string)) && fieldValue == null)
            {
                // return "";
                return null;
            }
            if (dataType == typeof(byte[]))
            {
                if (fieldValue == null)
                {
                    return "";
                }
                else
                {
                    byte[] bytes = (byte[])fieldValue;
                    StringBuilder hex = new StringBuilder(bytes.Length * 2);
                    foreach (byte b in bytes)
                        hex.AppendFormat("{0:x2}", b);
                    return "0x" + hex.ToString();
                }
            }
            if (dataType == typeof(XmlDocument))
            {
                if (fieldValue == null)
                {
                    return "";
                }
                else
                {
                    return ((XmlDocument)fieldValue).OuterXml.Replace("'", "''");
                }
            }
            if (dataType == typeof(DateTime) || dataType == typeof(DateTime?)) 
            {
                return string.Format("{0:s}", DateTime.Parse(fieldValue.ToString()));
            }
            if (dataType == typeof(DateTimeOffset)  || dataType == typeof(DateTimeOffset?))
            {
                return string.Format("{0:s}", DateTimeOffset.Parse(fieldValue.ToString()));
            }
            if (dataType == typeof(IList) || dataType == typeof(List<>) || dataType.Name.Equals("List`1"))
            {
                IList list = (IList)fieldValue;
                var item = list[0];
                string q = GetQuotable(item.GetType());
                fv = string.Format("{0}{1}{0}", q, item);
                for (int i = 1; i < list.Count; i++)
                {
                    fv += string.Format(",{0}{1}{0}", q, list[i]);
                }
                return fv;
            }
            else if (dataType == typeof(bool) || dataType == typeof(bool?))
            {
                return fieldValue == null ? "" : Convert.ToBoolean(fieldValue) ? "1" : "0";
            }
            else if (dataType == typeof(double) || dataType == typeof(double?))
            {
                return Convert.ToDouble(fieldValue).ToString(culture);
            }
            else if (dataType == typeof(decimal) || dataType == typeof(decimal?))
            {
                return Convert.ToDecimal(fieldValue).ToString(culture);
            }
            return fieldValue == null ? "" : fieldValue.ToString().Replace("'", "''");
        }



        public override string ToSql()
        {
            string tableAlias = Table.Alias;
            // return (!string.IsNullOrEmpty(tableAlias) ? "[" + tableAlias + "]." : "") + this.Name + (string.IsNullOrEmpty(this.Alias) || Value != null ? "" : " AS [" + Alias + "]");
            return (!string.IsNullOrEmpty(tableAlias) ? tableAlias + "." : "") + Name + (string.IsNullOrEmpty(Alias) || Value != null ? "" : " AS [" + Alias + "]");
        }
    }

    public class UpdateTable : Table
    {
        public UpdateTable()
        {
            Key.Parent = this;
        }
        public UpdateTable(SqlBuilder parent, string name, string schema = null)
            : base(parent, name, null, schema)
        {
            Key.Builder = parent;
            Key.Parent = this;
        }
        public override SqlBuilder Builder
        {
            get
            {
                return base.Builder;
            }
            set
            {
                base.Builder = value;
                Key.Builder = value;
            }
        }
        public override string ToSql()
        {
            if (FieldList.Count == 0)
            {
                return "";
            }
            List<ParameterField> fields = FieldList.OfType<ParameterField>().ToList();
            string sql = fields.First().ReferenceName + " = " + fields.First().ParameterName;
            foreach (ParameterField p in fields.Skip(1))
            {
                sql += ", " + p.ReferenceName + " = " + p.ParameterName;
            }
            return sql;
        }

        public PrimaryKey Key = new PrimaryKey();
        public TableParameterField Output = new TableParameterField();
    }

    public class TempTable : Table
    {
        public override string ReferenceName
        {
            get
            {
                return "#" + Name;
            }
        }

        private List<OrderBy> _orderByClause = new List<OrderBy>();

        public List<OrderBy> OrderByClause
        {
            get { return _orderByClause; }
            set { _orderByClause = value; }
        }


        private bool _outputTable = true;

        public bool OutputTable
        {
            get { return _outputTable; }
            set { _outputTable = value; }
        }

        public override string Alias
        {
            get
            {
                return _Alias ?? "#" + Name;
            }
            set
            {
                base.Alias = value;
            }
        }

        public override string Name
        {
            get
            {
                return base.Name;
            }
            set
            {
                base.Name = value;
            }
        }

    }

    public class InsertIntoTable : Table
    {
        public InsertIntoTable() { }
        public InsertIntoTable(SqlBuilder parent, string name)
            : base(parent, name, null)
        {

        }

        public override string ToSql()
        {
            if (FieldList.Count == 0)
            {
                return "";
            }
            string sql = FieldList[0].ReferenceName;
            for (int i = 1; i < FieldList.Count; i++)
            {
                sql += ", " + FieldList[i].ReferenceName;
            }
            return sql;
        }

        public override string ReferenceName
        {
            get
            {
                return Name;
            }
        }

        public string FieldParameters()
        {
            List<ParameterField> parameters = FieldList.Cast<ParameterField>().ToList();
            string sql = parameters.First().ParameterName;
            foreach (ParameterField p in parameters.Skip(1))
            {
                sql += ", " + p.ParameterName;
            }
            return sql;
        }

        public TableParameterField Output = new TableParameterField();

    }

    public class BaseTable : Table
    {

    }

    public class Table
    {
        public Table()
        {

        }
        public Table(SqlBuilder parent, string name, string alias, string schema = null)
        {
            Builder = parent;
            Name = name;
            if (!string.IsNullOrEmpty(alias))
            {
                Alias = alias;
            }
            Schema = schema;
        }
        public virtual SqlBuilder Builder
        {
            get;
            set;
        }
        private string _name = null;
        public virtual string Name
        {
            get
            {
                // return (Schema != null ? "[" + Schema + "]." : "") + _Name;
                return _name;
            }
            set
            {
                _name = value;
            }
        }

        public virtual string FullName
        {
            get
            {
                return (!string.IsNullOrEmpty(Schema) ? Schema + "." : "") + Name;
            }
        }

        public string Schema = null;

        protected string _Alias;
        public virtual string Alias
        {
            get
            {
                // return _Alias ?? (!string.IsNullOrEmpty(Schema) ? "[" + Schema + "]." : "") + Name;
                return _Alias ?? FullName;
            }

            set { _Alias = value; }
        }
        public List<Field> FieldList = new List<Field>();

        public virtual string ReferenceName
        {
            get
            {
                // return (Schema != null ? "[" + Schema + "]." : "") + Name + (!string.IsNullOrEmpty(_Alias) ? " [" + Alias + "]" : "");
                return (Schema != null ? Schema + "." : "") + Name + (!string.IsNullOrEmpty(_Alias) ? " " + Alias : "");
            }
        }
        public virtual string ToSql()
        {
            if (FieldList.Count == 0)
            {
                return "";
            }
            string sql = FieldList[0].ToSql();
            for (int i = 1; i < FieldList.Count; i++)
            {
                sql += ", " + FieldList[i].ToSql();
            }
            return sql;
        }
    }











}
