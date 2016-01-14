using System;

namespace TinySql.Attributes
{
    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public sealed class Pk : Attribute
    {
        public Pk()
        {

        }
    }

    [AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
    public sealed class Fk : Attribute
    {
        readonly string _toTable;
        readonly string _toSchema;
        readonly string _foreignKeyName;
        readonly string _toField;

        public Fk(string toTable, string toField = null, string toSchema = null, string foreignKeyName = null)
        {
            _toTable = toTable;
            _toSchema = toSchema;
            _foreignKeyName = foreignKeyName;
            _toField = null;
        }

        public string ToField
        {
            get { return _toField; }
        }
        public string ToTable
        {
            get
            {
                return _toTable;
            }
        }
        public string ToSchema
        {
            get { return _toSchema; }
        }

        public string ForeignKeyName
        {
            get
            {
                return _foreignKeyName;
            }
        }
    }

   
}
