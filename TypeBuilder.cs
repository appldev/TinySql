using System;
using System.Collections.Generic;
using System.Collections;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml;

namespace TinySql
{
    public class TypeCache
    {
        private TypeCache()
        {

        }
        private static TypeCache _instance = null;
        public static TypeCache Default
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new TypeCache();
                }
                return _instance;
            }
        }

        //private static ConcurrentDictionary<Type, SqlBuilder> _Select = null;




    }


    public static class TypeBuilder
    {
        public static SqlBuilder Select(Type objectType, string tableName = null, string[] properties = null, string[] excludeProperties = null, int? top = null, bool distinct = false)
        {
            SqlBuilder builder = SqlBuilder.Select(top, distinct);
            Table baseTable = null;
            if (string.IsNullOrEmpty(tableName))
            {
                baseTable = builder.From(objectType.Name);
            }
            else
            {
                baseTable = builder.From(tableName);
            }
            if (properties == null)
            {
                properties = objectType.GetProperties().Select(x => x.Name).Union(objectType.GetFields().Select(x => x.Name)).ToArray();
            }
            if (excludeProperties == null)
            {
                excludeProperties = new string[0];
            }
            foreach (string name in properties.Except(excludeProperties))
            {
                if (name.Equals("*"))
                {
                    baseTable.AllColumns(false);
                    return builder;
                }
                baseTable.Column(name);
            }
            return builder;
        }

        public static SqlBuilder Update<T>(T instance, string tableName = null, string schema = null, string[] properties = null, string[] excludeProperties = null, bool outputPrimaryKey = false)
        {
            UpdateTable table = SqlBuilder.Update()
                .Table(tableName ?? instance.GetType().Name,schema);

            Metadata.MetadataTable mt = SqlBuilder.DefaultMetadata.FindTable(tableName ?? instance.GetType().Name);

            if (properties == null)
            {
                properties = instance.GetType().GetProperties().Select(x => x.Name).ToArray();
            }
            if (excludeProperties != null)
            {
                properties = properties.Except(excludeProperties).ToArray();
            }

            foreach (Metadata.MetadataColumn col in mt.Columns.Values)
            {
                if (properties.Contains(col.Name) && !col.IsIdentity && !col.IsReadOnly)
                {
                    PropertyInfo prop = instance.GetType().GetProperty(col.Name);
                    if (prop.CanRead && prop.CanWrite)
                    {
                        table.Set(col.Name, prop.GetValue(instance), col.SqlDataType,prop.PropertyType);
                    }
                }
            }
            List<object> pk = new List<object>();
            mt.PrimaryKey.Columns.ForEach((col) =>
            {
                PropertyInfo prop = instance.GetType().GetProperty(col.Name);
                pk.Add(prop.GetValue(instance));
            });
            table.WithMetadata().WherePrimaryKey(pk.ToArray());
            if (outputPrimaryKey)
            {
                return table.Output().PrimaryKey().Builder();
            }
            else
            {
                return table.Builder();
            }
            
        }


        public static SqlBuilder Insert<T>(T instance, string tableName = null, string[] properties = null, string[] excludeProperties = null)
        {
            InsertIntoTable table = SqlBuilder.Insert()
                .Into(tableName ?? instance.GetType().Name);

            Metadata.MetadataTable mt = SqlBuilder.DefaultMetadata.FindTable(tableName ?? instance.GetType().Name);


            if (properties == null)
            {
                properties = instance.GetType().GetProperties().Select(x => x.Name).ToArray();
            }
            if (excludeProperties != null)
            {
                properties = properties.Except(excludeProperties).ToArray();
            }
            foreach (Metadata.MetadataColumn col in mt.Columns.Values)
            {
                if (properties.Contains(col.Name) && !col.IsIdentity && !col.IsReadOnly)
                {
                    PropertyInfo prop = instance.GetType().GetProperty(col.Name);
                    if (prop.CanRead && prop.CanWrite)
                    {
                        table.Value(prop.Name, prop.GetValue(instance));
                    }
                }
            }


            return table.Output().PrimaryKey().Builder();
        }

        public static SqlBuilder Update<TModel, TProperty>(this TableHelper<TModel> helper, TModel instance, Expression<Func<TModel, TProperty>> prop)
        {
            return helper.Table.Builder;
        }

        public static void UpdateEx<TModel, TProperty>(this ModelHelper<TModel> helper, Expression<Func<TModel, TProperty>> prop)
        {
            TProperty t = prop.Compile().Invoke(helper.Model);


        }

        public class ModelHelper<TModel>
        {
            public TModel Model { get; set; }
            public ModelHelper(TModel model)
            {
                Model = model;
            }
        }





        public static SqlBuilder Select<T>(string tableName = null, string[] properties = null, string[] excludeProperties = null, int? top = null, bool distinct = false)
        {

            return Select(typeof(T), tableName, properties, excludeProperties, top, distinct);
        }

        public static T PopulateObject<T>(T instance, DataTable dt, DataRow row, bool allowPrivateProperties, bool enforceTypesafety)
        {
            foreach (DataColumn col in dt.Columns)
            {
                BindingFlags flag = BindingFlags.Public;
                if (allowPrivateProperties)
                {
                    flag = flag | BindingFlags.NonPublic;
                }
                PropertyInfo prop = instance.GetType().GetProperty(col.ColumnName, BindingFlags.Instance | flag);
                FieldInfo field = null;
                if (prop == null)
                {
                    field = instance.GetType().GetField(col.ColumnName, BindingFlags.Instance | flag);
                    if (field != null)
                    {
                        if (field.FieldType == typeof(XmlDocument) && !row.IsNull(col))
                        {
                            if (col.DataType == typeof(string))
                            {
                                XmlDocument xml = new XmlDocument();
                                xml.LoadXml((string)row[col]);
                                field.SetValue(instance, xml);
                            }
                            else if (col.DataType == typeof(XmlDocument))
                            {
                                field.SetValue(instance, (XmlDocument)row[col]);
                            }
                        }
                        else if (!enforceTypesafety || field.FieldType == col.DataType)
                        {
                            if (row.IsNull(col))
                            {
                                if (!field.FieldType.IsValueType || Nullable.GetUnderlyingType(field.FieldType) != null)
                                {
                                    field.SetValue(instance, null);
                                }
                            }
                            else
                            {
                                field.SetValue(instance, row[col.ColumnName]);
                            }
                        }
                    }
                }
                else
                {
                    if (prop.CanWrite)
                    {
                        if (prop.PropertyType == typeof(XmlDocument) && !row.IsNull(col))
                        {
                            if (col.DataType == typeof(string))
                            {
                                XmlDocument xml = new XmlDocument();
                                xml.LoadXml((string)row[col]);
                                prop.SetValue(instance, xml, null);
                            }
                            else if (col.DataType == typeof(XmlDocument))
                            {
                                prop.SetValue(instance, (XmlDocument)row[col], null);
                            }
                        }
                        else if (!enforceTypesafety || prop.PropertyType == col.DataType)
                        {
                            if (row.IsNull(col))
                            {
                                if (!prop.PropertyType.IsValueType || Nullable.GetUnderlyingType(prop.PropertyType) != null)
                                {
                                    prop.SetValue(instance, null, null);
                                }
                            }
                            else
                            {
                                if (col.DataType == typeof(decimal) && (prop.PropertyType == typeof(double) || prop.PropertyType == typeof(double?)))
                                {
                                    prop.SetValue(instance, Convert.ToDouble(row[col.ColumnName]), null);
                                }
                                else if (prop.PropertyType == typeof(bool))
                                {
                                    prop.SetValue(instance, Convert.ToBoolean(row[col.ColumnName]), null);
                                }
                                else
                                {
                                    prop.SetValue(instance, row[col.ColumnName], null);
                                }

                            }
                        }
                    }
                    else
                    {
                        continue;
                    }
                }
            }
            return instance;
        }

        public static List<T> PopulateObject<T>(ResultTable table)
        {
            List<T> list = new List<T>();
            foreach (RowData row in table)
            {
                list.Add(PopulateObject<T>(row));
            }
            return list;
        }

        private static object PopulateObject(Type t, RowData row)
        {
            object instance = Activator.CreateInstance(t);
            foreach (string prop in row.GetDynamicMemberNames())
            {
                PropertyInfo p = instance.GetType().GetProperty(prop);
                if (p != null && p.CanWrite)
                {
                    object o = row.Column(prop);
                    if (o is ResultTable && p.PropertyType.GetInterface("IList",true) != null)
                    {
                        Type listType = typeof(List<>);
                        Type[] args = p.PropertyType.GetGenericArguments();
                        Type genericList = listType.MakeGenericType(args);
                        object listInstance = Activator.CreateInstance(genericList);
                        foreach (RowData r in (o as ResultTable))
                        {
                            ((IList)listInstance).Add(PopulateObject(args[0], r));
                        }
                        p.SetValue(instance, listInstance);
                    }
                    else
                    {
                        p.SetValue(instance, o);
                    }
                }
            }
            return instance;
        }

        public static T PopulateObject<T>(RowData row)
        {
            return (T)PopulateObject(typeof(T), row);
        }


        public static T PopulateObject<T>(DataTable dt, DataRow row, bool allowPrivateProperties, bool enforceTypesafety, bool useDefaultConstructor = true)
        {
            T instance = default(T);
            if (useDefaultConstructor)
            {
                instance = Activator.CreateInstance<T>();
                return PopulateObject<T>(instance, dt, row, allowPrivateProperties, enforceTypesafety);
            }
            else
            {
                object o = Activator.CreateInstance(typeof(T), new object[] { row });
                if (o == null)
                {
                    o = Activator.CreateInstance(typeof(T), new object[] { dt, row });
                }
                if (o == null)
                {
                    o = Activator.CreateInstance(typeof(T), new object[] { row, dt });
                }
                if (o == null)
                {
                    o = Activator.CreateInstance(typeof(T), new object[] { dt });
                }
                if (o != null)
                {
                    return (T)o;
                }
                else
                {
                    throw new InvalidOperationException(string.Format("The type {0} does not provide a valid constructor for a DataRow and/or DataTable object", typeof(T).FullName));
                }

            }



        }
    }
}
