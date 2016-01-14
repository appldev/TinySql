using System;
using TinySql.Metadata;
using System.Linq;

namespace TinySql
{
    public static class ResultExtensions
    {

        public static SqlBuilder Select(this RowData row, string listName)
        {
            SqlBuilder builder = row.Metadata.ToSqlBuilder(listName);
            builder.WhereConditions = row.PrimaryKey(builder);
            return builder;
        }

        public static SqlBuilder Update(this RowData row, bool onlyChanges = false, bool outputPrimaryKey = false, string[] outputFields = null)
        {
            if (onlyChanges && !row.HasChanges)
            {
                return null;
            }
            SqlBuilder builder = SqlBuilder.Update();
            string tableName = row.Table;
            string schema = null;
            if (tableName.IndexOf('.') > 0)
            {
                schema = tableName.Substring(0, tableName.IndexOf('.'));
                tableName = tableName.Substring(tableName.IndexOf('.') + 1);
            }
            UpdateTable up = builder.Table(tableName, schema);
            MetadataTable mt = row.Metadata;
            if (onlyChanges)
            {
                foreach (string key in row.ChangedValues.Keys)
                {
                    object o;
                    MetadataColumn c;
                    if (row.ChangedValues.TryGetValue(key, out o) && mt.Columns.TryGetValue(key, out c) && !c.IsReadOnly)
                    {
                        SqlStatementExtensions.Set(up, key, o, c.SqlDataType, c.DataType, c.Length, c.Precision, c.Scale);
                    }
                    else
                    {
                        throw new InvalidOperationException("Cannot get the changed column " + key);
                    }
                }
            }
            else
            {
                foreach (string key in row.Columns)
                {
                    MetadataColumn c;
                    if (mt.Columns.TryGetValue(key, out c) && !c.IsReadOnly)
                    {
                        SqlStatementExtensions.Set(up, key, row.Column(key), c.SqlDataType, c.DataType, c.Length, c.Precision, c.Scale);
                    }

                }
            }

            if (outputPrimaryKey)
            {
                TableParameterField tpf = up.Output();
                foreach (MetadataColumn key in mt.PrimaryKey.Columns)
                {
                    SqlStatementExtensions.Column(tpf, key.Name, key.SqlDataType, key.Length, key.Precision, key.Scale);
                }
            }
            if (outputFields != null && outputFields.Length > 0)
            {
                TableParameterField tpf = up.Output();
                foreach (string s in outputFields)
                {
                    MetadataColumn c = mt[s];
                    SqlStatementExtensions.Column(tpf, s, c.SqlDataType, c.Length, c.Precision, c.Scale);
                }
            }
            builder.WhereConditions = row.PrimaryKey(builder);
            return builder;

        }

        public static SqlBuilder Update(this SqlBuilder builder, RowData row, string[] output = null)
        {
            if (!row.HasChanges)
            {
                return builder;
            }
            string tableName = row.Table;
            string schema = null;
            if (tableName.IndexOf('.') > 0)
            {
                schema = tableName.Substring(0, tableName.IndexOf('.'));
                tableName = tableName.Substring(tableName.IndexOf('.') + 1);
            }
            UpdateTable up = builder.Table(tableName, schema);
            MetadataTable mt = row.Metadata;
            foreach (string key in row.ChangedValues.Keys)
            {
                MetadataColumn c = mt[key];
                SqlStatementExtensions.Set(up, key, row.ChangedValues[key], c.SqlDataType, c.DataType, c.Length, c.Scale);
            }
            if (output != null && output.Length > 0)
            {
                TableParameterField tpf = up.Output();
                foreach (string s in output)
                {
                    MetadataColumn c = mt[s];
                    SqlStatementExtensions.Column(tpf, s, c.SqlDataType, c.Length, c.Scale);
                }
            }
            builder.WhereConditions = row.PrimaryKey(builder);
            return builder;
        }

        public static RowData LoadMissingColumns<T>(this RowData row)
        {
            int cols = row.OriginalValues.Keys.Count(x => !x.StartsWith("__"));
            if (cols < row.Metadata.Columns.Count)
            {
                foreach (MetadataColumn col in row.Metadata.Columns.Values.Where(x => !row.OriginalValues.Keys.Contains(x.Name)))
                {
                    if (col.DataType == typeof(T))
                    {
                        object o = col.DataType.IsValueType ? Activator.CreateInstance(col.DataType) : null;
                        row.OriginalValues.AddOrUpdate(col.Name, o, (k, v) => { return o; });
                        row.Columns.Add(col.Name);
                    }
                }
            }
            return row;
        }



    }
}
