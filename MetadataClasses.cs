using Newtonsoft.Json;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Xml.Serialization;

namespace TinySql.Metadata
{
    [Serializable]
    public class MetadataDatabase
    {
        public Guid Id = Guid.NewGuid();
        public string Name { get; set; }
        public string Server { get; set; }
        public SqlBuilder Builder { get; set; }

        public long Version { get; set; }

        public ConcurrentDictionary<string, MetadataTable> Tables = new ConcurrentDictionary<string, MetadataTable>();
        // public ConcurrentDictionary<int, MetadataForeignKey> ForeignKeys = new ConcurrentDictionary<int, MetadataForeignKey>();
        // public ConcurrentDictionary<string, List<int>> InversionKeys = new ConcurrentDictionary<string, List<int>>();


        public MetadataTable this[string tableName]
        {
            get
            {
                MetadataTable table;
                if (Tables.TryGetValue(tableName, out table))
                {
                    return table;
                }
                else
                {
                    return null;
                }
            }
        }

        public MetadataTable FindTable(string name, StringComparison compareOption = StringComparison.OrdinalIgnoreCase)
        {
            MetadataTable mt = null;
            string schema = null;
            if (!name.Contains('.'))
            {
                schema = "dbo.";
            }
            if (Tables.TryGetValue(schema + name, out mt))
            {
                return mt;
            }
            string[] keys = Tables.Keys.Where(x => x.EndsWith(name, compareOption)).ToArray();
            if (keys.Length != 1)
            {
                return null;
            }
            return this[keys[0]];

        }


    }
    [Serializable]
    public class MetadataTable
    {
        public MetadataTable()
        {

        }
        public int Id { get; set; }
        //public MetadataDatabase Parent { get; set; }
        public string Schema { get; set; }
        public string Name { get; set; }

        public string Fullname
        {
            get
            {
                return (!string.IsNullOrEmpty(Schema) ? Schema + "." + Name : Name);
            }
        }

        

        public MetadataColumn this[string columnName]
        {
            get
            {
                MetadataColumn column;
                if (Columns.TryGetValue(columnName, out column))
                {
                    return column;
                }
                else
                {
                    return null;
                }
            }
        }

        private ConcurrentDictionary<string, MetadataColumn> _columns = new ConcurrentDictionary<string, MetadataColumn>();

        public ConcurrentDictionary<string, MetadataColumn> Columns
        {
            get { return _columns; }
            set { _columns = value; }
        }


        #region Extended Properties

        private ConcurrentDictionary<int, string> _displayNames = new ConcurrentDictionary<int, string>();

        public ConcurrentDictionary<int, string> DisplayNames
        {
            get { return _displayNames; }
            set { _displayNames = value; }
        }

        public string DisplayName
        {
            get
            {
                return GetDisplayName(SqlBuilder.DefaultCulture.LCID);
            }
        }
        public string GetDisplayName(int lcid)
        {
            string value;
            if (_displayNames.TryGetValue(SqlBuilder.DefaultCulture.LCID, out value))
            {
                return value;
            }
            return Name;
        }

        private string _titleColumn = null;
        public string TitleColumn
        {
            get { return _titleColumn; }
            set { _titleColumn = value; }
        }

        private ConcurrentDictionary<string, List<string>> _listDefinitions = new ConcurrentDictionary<string,List<string>>();

        public ConcurrentDictionary<string, List<string>> ListDefinitions
        {
            get { return _listDefinitions; }
            set { _listDefinitions = value; }
        }
        



        #endregion

        public Key PrimaryKey
        {
            get
            {
                return Indexes.Values.FirstOrDefault(x => x.IsPrimaryKey == true);
            }
        }

        public IEnumerable<MetadataForeignKey> FindForeignKeys(MetadataColumn column, string referencedTable = null)
        {
            foreach (MetadataForeignKey fk in ForeignKeys.Values)
            {
                if (fk.ColumnReferences.Select(x => x.Column).Any(x => x.Equals(column)))
                {
                    if (referencedTable == null || fk.ReferencedTable.Equals(referencedTable, StringComparison.OrdinalIgnoreCase))
                    {
                        yield return fk;
                    }
                }
            }
        }

        public ConcurrentDictionary<string, MetadataForeignKey> ForeignKeys = new ConcurrentDictionary<string, MetadataForeignKey>();
        public ConcurrentDictionary<string, Key> Indexes = new ConcurrentDictionary<string, Key>();
    }
    [Serializable]
    public class MetadataForeignKey
    {
        public int Id { get; set; }

        public MetadataTable Parent { get; set; }
        public MetadataDatabase Database { get; set; }

        public string Name { get; set; }
        public List<MetadataColumnReference> ColumnReferences = new List<MetadataColumnReference>();
        public string ReferencedSchema { get; set; }
        public string ReferencedTable { get; set; }
        public string ReferencedKey { get; set; }

        private bool _isVirtual = false;

        public bool IsVirtual
        {
            get { return _isVirtual; }
            set { _isVirtual = value; }
        }

    }

    [Serializable]
    public class MetadataColumnReference
    {
        //public MetadataForeignKey Parent { get; set; }
        //public MetadataDatabase Database { get; set; }

        public string Name { get; set; }

        public MetadataColumn Column { get; set; }

        public MetadataColumn ReferencedColumn { get; set; }
    }

    [Serializable]
    public class Key
    {
        public int Id { get; set; }

        public MetadataTable Parent { get; set; }
        public MetadataDatabase Database { get; set; }

        public string Name { get; set; }
        public List<MetadataColumn> Columns = new List<MetadataColumn>();
        public bool IsUnique { get; set; }
        public bool IsPrimaryKey { get; set; }


    }

    public class ForeignKeyCollection : List<MetadataForeignKey> //  IEnumerable<MetadataForeignKey>
    {
        public void AddKey(MetadataForeignKey value, string inversionKey)
        {
            Add(value);
            List<int> keys = new List<int>(new int[] { value.Id });
            //this.Database.InversionKeys.AddOrUpdate(InversionKey, keys, (key, existing) =>
            //{
            //    existing.Add(Value.ID);
            //    return existing;
            //});

        }

        public MetadataTable Parent { get; set; }
        public MetadataDatabase Database { get; set; }

        //public IEnumerator<MetadataForeignKey> GetEnumerator()
        //{
        //    foreach (int i in list)
        //    {
        //        MetadataForeignKey FK;
        //        if (this.Database.ForeignKeys.TryGetValue(i,out FK))
        //        {
        //            yield return FK;
        //        }
        //    }
        //}

        //IEnumerator IEnumerable.GetEnumerator()
        //{
        //    return GetEnumerator();
        //}
    }

    [Serializable]
    public class MetadataColumn
    {
        public int Id { get; set; }
        public MetadataTable Parent { get; set; }
        //public MetadataDatabase Database {get; set;}

        public string Name { get; set; }
        public string Collation { get; set; }
        public SqlDbType SqlDataType { get; set; }
        public int Length { get; set; }

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
        private string _dataTypeName;
        public string DataTypeName
        {
            get { return _dataTypeName; }
            set
            {
                _dataTypeName = value;
                if (_dataType == null)
                {
                    if (!string.IsNullOrEmpty(_dataTypeName))
                    {
                        _dataType = Type.GetType(_dataTypeName);
                    }
                    
                }
            }
        }

        private Type _dataType = null;
        [XmlIgnore]
        public Type DataType
        {
            get { return _dataType; }
            set
            {
                _dataType = value;
                DataTypeName = _dataType != null ? _dataType.FullName : "Virtual";
            }
        }

        public string Default { get; set; }
        public bool IsComputed { get; set; }
        public string ComputedText { get; set; }
        public bool IsIdentity { get; set; }
        public bool IsRowGuid { get; set; }
        public long IdentitySeed { get; set; }
        public long IdentityIncrement { get; set; }
        public bool IsPrimaryKey { get; set; }
        public bool IsForeignKey { get; set; }
        public bool Nullable { get; set; }

        [XmlIgnore]
        [JsonIgnore]
        public bool IsReadOnly
        {
            get
            {
                return IsComputed || IsIdentity || IsRowGuid;
            }
        }


        #region Extended Properties
        
        private ConcurrentDictionary<int, string> _displayNames = new ConcurrentDictionary<int, string>();

        public ConcurrentDictionary<int, string> DisplayNames
        {
            get { return _displayNames; }
            set { _displayNames = value; }
        }

        public string DisplayName
        {
            get
            {
                return GetDisplayName(SqlBuilder.DefaultCulture.LCID);
            }
        }
        public string GetDisplayName(int lcid)
        {
            string value;
            if (_displayNames.TryGetValue(SqlBuilder.DefaultCulture.LCID, out value))
            {
                return value;
            }
            return Name;
        }

        private string[] _includeColumns = null;

        public string[] IncludeColumns
        {
            get { return _includeColumns; }
            set { _includeColumns = value; }
        }

        

        #endregion

        public void PopulateField<T>(T field) where T : class
        {
            Field f = field as Field;
            f.Name = Name;
            f.MaxLength = Length;
            f.Scale = Scale;
            f.Precision = Precision;
            f.SqlDataType = SqlDataType;
            
        }

    }




}
