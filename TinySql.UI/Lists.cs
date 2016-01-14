using System.Collections.Generic;
using System.Data;

namespace TinySql.UI
{
    public enum ListTypes
    {
        Primary = 1,
        Secondary = 2,
        Integrated = 3,
        Lookup = 4,
        Custom = 5
    }

    public enum ColumnDataTypes
    {
        Date,
        Currency,
        CurrencyWithHtml,
        Numeric,
        NumericWithHtml,
        Text,
        Html
    }

    #region Defaults
        public sealed class ListDefaults
        {
            private ListDefaults() {

            }

            private static ListDefaults instance = null;
            public static ListDefaults Default
            {
                get {
                    if (instance == null)
                    {
                        instance = new ListDefaults();
                    }
                    return instance;
                }
            }

            private string _ListViewUrl = "~/Views/TinySql/Lists/List.cshtml";
            public string ListViewUrl
            {
                get { return _ListViewUrl; }
                set { _ListViewUrl = value; }
            }


        }

        #endregion


    public sealed class ListBuilder
    {
        private ListTypes _ListType = ListTypes.Primary;
        public ListTypes ListType
        {
            get { return _ListType; }
            set { _ListType = value; }
        }

        private bool _AllowNew = true;

        public bool AllowNew
        {
            get { return _AllowNew; }
            set { _AllowNew = value; }
        }
        private bool _AllowDelete = true;

        public bool AllowDelete
        {
            get { return _AllowDelete; }
            set { _AllowDelete = value; }
        }
        private bool _AllowEdit = true;

        public bool AllowEdit
        {
            get { return _AllowEdit; }
            set { _AllowEdit = value; }
        }

        private string _CustomName = null;

        public string CustomName
        {
            get { return _CustomName; }
            set { _CustomName = value; }
        }

        private SqlBuilder _Builder = null;

        public SqlBuilder Builder
        {
            get { return _Builder; }
            set { _Builder = value; }
        }

        public string TableName { get; set; }

        public string Title { get; set; }

        private List<ListColumn> _Columns = new List<ListColumn>();
        public List<ListColumn> Columns
        {
            get { return _Columns; }
            set { _Columns = value; }
        }

        private string _ListViewUrl = null;
        public string ListViewUrl
        {
            get { return _ListViewUrl ?? ListDefaults.Default.ListViewUrl; }
            set { _ListViewUrl = value; }
        }
    }

    public sealed class ListColumn
    {
        private bool _IsVisible = true;
        public bool IsVisible
        {
            get { return _IsVisible; }
            set { _IsVisible = value; }
        }
        public string Visible
        {
            get
            {
                return _IsVisible ? "true" : "false";
            }
        }

        internal static ColumnDataTypes GetColumnDataType(SqlDbType SqlType)
        {
            switch (SqlType)
            {
                
                case SqlDbType.BigInt:
                case SqlDbType.Bit:
                case SqlDbType.Int:
                case SqlDbType.Decimal:
                case SqlDbType.Float:
                case SqlDbType.Real:
                case SqlDbType.SmallInt:
                case SqlDbType.TinyInt:
                    return ColumnDataTypes.Numeric;
    
                case SqlDbType.Date:
                case SqlDbType.DateTime:
                case SqlDbType.DateTime2:
                case SqlDbType.DateTimeOffset:
                case SqlDbType.SmallDateTime:
                case SqlDbType.Time:
                    return ColumnDataTypes.Date;
                
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                    return ColumnDataTypes.Currency;
                    
                case SqlDbType.Image:
                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.NText:
                case SqlDbType.NVarChar:
                case SqlDbType.Text:
                case SqlDbType.VarChar:
                case SqlDbType.VarBinary:
                case SqlDbType.UniqueIdentifier:
                case SqlDbType.Structured:
                case SqlDbType.Udt:
                case SqlDbType.Xml:
                case SqlDbType.Variant:
                case SqlDbType.Binary:
                case SqlDbType.Timestamp:
                default:
                    return ColumnDataTypes.Text;

            }
        }

        private ColumnDataTypes _ColumnDataType = ColumnDataTypes.Text;
        public ColumnDataTypes ColumnDataType
        {
            get { return _ColumnDataType; }
            set { _ColumnDataType = value; }
        }
        public string DataType
        {
            get
            {
                switch (ColumnDataType)
                {
                    case ColumnDataTypes.Date:
                        return "date";
                    
                    case ColumnDataTypes.Currency:
                        return "num-fmt";
                    
                    case ColumnDataTypes.CurrencyWithHtml:
                        return "num-fmt-html";
                    
                    case ColumnDataTypes.Numeric:
                        return "num";
                    
                    case ColumnDataTypes.NumericWithHtml:
                        return "num-html";
                    
                    case ColumnDataTypes.Html:
                        return "html";
                    
                    case ColumnDataTypes.Text:
                    default:
                        return "string";
                }
            }
        }

        public string ColumnName { get; set; }
        public string DisplayName { get; set; }

    }




}
