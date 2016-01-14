using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Transactions;
using TinySql.Cache;
using TinySql.Metadata;

namespace TinySql
{
    public static class Data
    {
        #region Execute methods

        public static ResultTable Execute(this SqlBuilder builder, int timeoutSeconds = 30, bool withMetadata = true, ResultTable.DateHandlingEnum? dateHandling = null, bool useCache = true, string useHierachyField = null, params object[] format)
        {
            if (useCache && CacheProvider.UseResultCache)
            {
                if (CacheProvider.ResultCache.IsCached(builder))
                {
                    return CacheProvider.ResultCache.Get(builder);
                }
            }
            ResultTable result = new ResultTable(builder, timeoutSeconds, withMetadata, dateHandling, useHierachyField, format);
            if (CacheProvider.UseResultCache)
            {
                CacheProvider.ResultCache.Add(builder, result);
            }
            return result;

        }



        public static ResultTable Execute(this List<SqlBuilder> builders, int timeoutSeconds = 30)
        {
            return Execute(builders.ToArray(), timeoutSeconds);
        }

        private static ResultTable ExecuteRelatedInternal(SqlBuilder builder, Dictionary<string, RowData> results)
        {
            if (results.Count > 0)
            {
                MetadataTable mt = builder.BaseTable().WithMetadata().Model;
                foreach (string key in results.Keys)
                {
                    foreach (MetadataForeignKey fk in mt.ForeignKeys.Values.Where(x => (x.ReferencedSchema + "." + x.ReferencedTable).Equals(key, StringComparison.OrdinalIgnoreCase)))
                    {
                        RowData row = results[key];
                        foreach (MetadataColumnReference mcr in fk.ColumnReferences)
                        {
                            if (row.Columns.Contains(mcr.Column.Name))
                            {
                                Field f = builder.BaseTable().FindField(mcr.Column.Name);
                                if (f != null)
                                {
                                    f.Value = row.Column(mcr.Column.Name);
                                }
                                else
                                {
                                    (builder.BaseTable() as InsertIntoTable).Value(mcr.Column.Name, row.Column(mcr.Column.Name), SqlDbType.VarChar);
                                }
                            }
                        }
                    }
                }
            }
            DataTable dt = new DataTable();
            ResultTable table = new ResultTable();
            using (SqlConnection context = new SqlConnection(builder.ConnectionString))
            {
                context.Open();
                SqlCommand cmd = new SqlCommand(builder.ToSql(), context);
                SqlDataAdapter adapter = new SqlDataAdapter(cmd) {AcceptChangesDuringFill = false};
                adapter.Fill(dt);
                context.Close();
            }

            if (builder.SubQueries.Count > 0)
            {
                Dictionary<string, RowData> subresults = new Dictionary<string, RowData>(results);
                if (dt.Rows.Count > 0)
                {
                    MetadataTable mt = builder.BaseTable().WithMetadata().Model;
                    if (!subresults.ContainsKey(mt.Fullname))
                    {
                        ResultTable rt = new ResultTable(dt, ResultTable.DateHandlingEnum.None);
                        RowData row = rt.First();
                        table.Add(row);
                        subresults.Add(mt.Fullname, row);
                    }
                }
                foreach (SqlBuilder sb in builder.SubQueries.Values)
                {
                    ResultTable sub = ExecuteRelatedInternal(sb, subresults);
                    foreach (RowData row in sub)
                    {
                        table.Add(row);
                    }
                }
            }
            return table;


        }

        public static ResultTable Execute(this SqlBuilder[] builders, int timeoutSeconds = 30)
        {
            ResultTable table = new ResultTable();
            using (TransactionScope trans = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions()
            {
                IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted,
                Timeout = TimeSpan.FromSeconds(timeoutSeconds)
            }))
            {
                try
                {
                    foreach (SqlBuilder mainBuilder in builders)
                    {
                        DataTable dt = new DataTable();
                        using (SqlConnection context = new SqlConnection(mainBuilder.ConnectionString))
                        {
                            context.Open();
                            SqlCommand cmd = new SqlCommand(mainBuilder.ToSql(), context);
                            SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                            adapter.AcceptChangesDuringFill = false;
                            adapter.Fill(dt);
                            context.Close();
                        }

                        if (mainBuilder.SubQueries.Count > 0)
                        {
                            Dictionary<string, RowData> results = new Dictionary<string, RowData>();
                            if (dt.Rows.Count > 0)
                            {
                                MetadataTable mt = mainBuilder.BaseTable().WithMetadata().Model;
                                if (!results.ContainsKey(mt.Fullname))
                                {
                                    ResultTable rt = new ResultTable(dt, ResultTable.DateHandlingEnum.None);
                                    RowData row = rt.First();
                                    results.Add(mt.Fullname, row);
                                    table.Add(row);
                                }
                            }
                            foreach (SqlBuilder sb in mainBuilder.SubQueries.Values)
                            {
                                ResultTable sub = ExecuteRelatedInternal(sb, results);
                                foreach (RowData row in sub)
                                {
                                    table.Add(row);
                                }
                            }
                        }
                    }
                }
                catch (TransactionException)
                {
                    trans.Dispose();
                    throw;
                }
                catch (SqlException)
                {
                    trans.Dispose();
                    throw;
                }
                catch (ApplicationException)
                {
                    trans.Dispose();
                    throw;
                }
                trans.Complete();
            }

            return table;


        }



        public static DataTable DataTable(this SqlBuilder builder, string connectionString = null, int timeoutSeconds = 30, params object[] format)
        {
            connectionString = connectionString ?? builder.ConnectionString ?? SqlBuilder.DefaultConnection;
            if (connectionString == null)
            {
                throw new InvalidOperationException("The ConnectionString must be set on the Execute Method or on the SqlBuilder");
            }
            DataTable dt = new DataTable();
            using (TransactionScope trans = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions()
            {
                IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted,
                Timeout = TimeSpan.FromMinutes(1)
            }))
            {
                try
                {
                    using (SqlConnection context = new SqlConnection(connectionString))
                    {
                        context.Open();
                        SqlCommand cmd = new SqlCommand(builder.ToSql(format), context);
                        cmd.CommandTimeout = timeoutSeconds;
                        SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                        adapter.Fill(dt);
                        context.Close();
                    }
                }
                catch (TransactionException exTrans)
                {
                    trans.Dispose();
                    throw;
                }
                trans.Complete();
            }
            if (builder.StatementType == SqlBuilder.StatementTypes.Procedure)
            {
                FillBuilderFromProcedureOutput(builder, dt);
            }
            return dt;
        }

        private static void FillBuilderFromProcedureOutput(SqlBuilder builder, DataTable dt)
        {
            int count = builder.Procedure.Parameters.Count(x => x.IsOutput);
            if (count > 0 && dt.Rows.Count == 1 && dt.Columns.Count == count)
            {
                int idx = 0;
                foreach (ParameterField field in builder.Procedure.Parameters.Where(x => x.IsOutput == true))
                {
                    field.Value = dt.Rows[0][idx];
                    idx++;
                }
            }
        }

        public static DataSet DataSet(this SqlBuilder builder, string connectionString = null, int timeoutSeconds = 60, params object[] format)
        {
            connectionString = connectionString ?? builder.ConnectionString ?? SqlBuilder.DefaultConnection;
            if (connectionString == null)
            {
                throw new InvalidOperationException("The ConnectionString must be set on the Execute Method or on the SqlBuilder");
            }
            DataSet ds = new DataSet();
            using (TransactionScope trans = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions()
            {
                IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted,
                Timeout = TimeSpan.FromSeconds(timeoutSeconds)
            }))
            {
                try
                {
                    using (SqlConnection context = new SqlConnection(connectionString))
                    {
                        context.Open();
                        SqlCommand cmd = new SqlCommand(builder.ToSql(format), context);
                        SqlDataAdapter adapter = new SqlDataAdapter(cmd);
                        adapter.AcceptChangesDuringFill = false;
                        adapter.Fill(ds);
                        context.Close();
                    }
                }
                catch (TransactionException exTrans)
                {
                    trans.Dispose();
                    throw exTrans;
                }
                trans.Complete();
            }
            if (builder.StatementType == SqlBuilder.StatementTypes.Procedure)
            {
                FillBuilderFromProcedureOutput(builder, ds.Tables[0]);
            }
            return ds;
        }

        private static int ExecuteNonQueryInternal(SqlBuilder builder, string connectionString, int timeout = 30)
        {
            using (SqlConnection context = new SqlConnection(connectionString))
            {
                context.Open();
                SqlCommand cmd = null;
                if (builder.StatementType != SqlBuilder.StatementTypes.Procedure)
                {
                    cmd = new SqlCommand(builder.ToSql(builder.Format), context);
                }
                else
                {
                    cmd = new SqlCommand(builder.Procedure.Name, context);
                    cmd.CommandType = CommandType.StoredProcedure;
                    foreach (ParameterField par in builder.Procedure.Parameters)
                    {
                        cmd.Parameters.Add(ToSqlParameter(par));
                    }
                }
                cmd.CommandTimeout = timeout;
                int i = cmd.ExecuteNonQuery();
                context.Close();
                if (builder.StatementType == SqlBuilder.StatementTypes.Procedure && builder.Procedure.Parameters.Count(x => x.IsOutput) > 0)
                {
                    foreach (ParameterField par in builder.Procedure.Parameters.Where(x => x.IsOutput))
                    {
                        par.Value = cmd.Parameters[par.ParameterName].Value;
                    }
                }
                return i;
            }
        }

        private static SqlParameter ToSqlParameter(ParameterField field)
        {
            SqlParameter p = new SqlParameter()
            {
                ParameterName = field.ParameterName,
                SqlDbType = field.SqlDataType,
                Precision = field.Precision >= 0 ? (byte)field.Precision : (byte)0,
                Scale = field.Scale >= 0 ? (byte)field.Scale : (byte)0,
                Size = field.MaxLength >= 0 ? field.MaxLength : 0,
                Direction = field.IsOutput ? ParameterDirection.Output : ParameterDirection.Input
            };
            if (!field.IsOutput)
            {
                // object o = ParameterField.GetFieldValue(field.DataType, field.Value, field.Builder.Culture);
                // p.Value = o == null ? DBNull.Value : o;
                p.Value = field.Value == null ? DBNull.Value : field.Value;
            }
            return p;
        }


        public static int ExecuteNonQuery(this SqlBuilder builder, string connectionString = null, int timeoutSeconds = 30)
        {
            return new SqlBuilder[] { builder }.ExecuteNonQuery(connectionString, timeoutSeconds);
        }

        public static int ExecuteNonQuery(this SqlBuilder[] builders, string connectionString = null, int timeoutSeconds = 30)
        {
            connectionString = connectionString ?? SqlBuilder.DefaultConnection;
            if (connectionString == null)
            {
                throw new InvalidOperationException("The ConnectionString must be set on the Execute Method or on the SqlBuilder");
            }
            int rowsAffected = 0;
            using (TransactionScope trans = new TransactionScope(TransactionScopeOption.RequiresNew, new TransactionOptions()
            {
                IsolationLevel = System.Transactions.IsolationLevel.ReadCommitted,
                Timeout = TimeSpan.FromSeconds(timeoutSeconds)
            }))
            {
                try
                {
                    // using (SqlConnection context = new SqlConnection(ConnectionString))
                    //{
                    //  context.Open();
                    rowsAffected += builders.Sum(builder => ExecuteNonQueryInternal(builder, builder.ConnectionString ?? connectionString, timeoutSeconds));
                    //context.Close();
                    //}
                }
                catch (TransactionException exTrans)
                {
                    trans.Dispose();
                    throw exTrans;
                }
                catch (SqlException exSql)
                {
                    trans.Dispose();
                    throw exSql;
                }
                catch (ApplicationException exApplication)
                {
                    trans.Dispose();
                    throw exApplication;
                }
                trans.Complete();
            }
            return rowsAffected;
        }

        #endregion

        #region FirstOrDefault<T> Methods
        public static T FirstOrDefault<T>(this SqlBuilder builder, string connectionString = null, int timeoutSeconds = 30, bool allowPrivateProperties = false, bool enforceTypesafety = true, params object[] format)
        {
            DataTable dt = DataTable(builder, connectionString, timeoutSeconds, format);
            if (dt.Rows.Count == 0)
            {
                return default(T);
            }
            return TypeBuilder.PopulateObject<T>(dt, dt.Rows[0], allowPrivateProperties, enforceTypesafety);
        }

        #endregion

            #region List<T> Methods

        public static List<T> All<T>(string tableName = null, int? top = null, bool distinct = false, string connectionString = null, int timeoutSeconds = 30, bool allowPrivateProperties = false, bool enforceTypesafety = true)
        {
            return List<T>(null, All<T>(tableName, top, distinct, connectionString, timeoutSeconds), allowPrivateProperties, enforceTypesafety);
        }

        public static List<T> List<T>(string tableName = null, string[] properties = null, string[] excludeProperties = null, int? top = null, bool distinct = false, string connectionString = null, int timeoutSeconds = 30, bool allowPrivateProperties = false, bool enforceTypesafety = true, params object[] format)
        {
            SqlBuilder builder = TypeBuilder.Select<T>(tableName, properties, excludeProperties, top, distinct);
            return builder.List<T>(connectionString, timeoutSeconds, allowPrivateProperties, enforceTypesafety, format);
        }

        public static List<T> List<T>(this SqlBuilder builder, DataTable dataTable, bool allowPrivateProperties, bool enforceTypesafety)
        {
            List<T> list = new List<T>();
            foreach (DataRow row in dataTable.Rows)
            {
                list.Add(TypeBuilder.PopulateObject<T>(dataTable, row, allowPrivateProperties, enforceTypesafety));
            }
            return list;
        }

        public static TS List<T, TS>(this SqlBuilder builder, DataTable dataTable, bool allowPrivateProperties, bool enforceTypesafety)
        {
            ICollection<T> list = Activator.CreateInstance<TS>() as ICollection<T>;
            foreach (DataRow row in dataTable.Rows)
            {
                list.Add(TypeBuilder.PopulateObject<T>(dataTable, row, allowPrivateProperties, enforceTypesafety));
            }
            return (TS)list;
        }

        public static TS List<T, TS>(this SqlBuilder builder, string connectionString = null, int timeoutSeconds = 30, bool allowPrivateProperties = false, bool enforceTypesafety = true, params object[] format)
        {
            DataTable dt = DataTable(builder, connectionString, timeoutSeconds, format);
            DataSet ds = DataSet(builder, connectionString, timeoutSeconds, format);
            return List<T, TS>(builder, dt, allowPrivateProperties, enforceTypesafety);


        }


        public static List<T> List<T>(this SqlBuilder builder, string connectionString = null, int timeoutSeconds = 30, bool allowPrivateProperties = false, bool enforceTypesafety = true, params object[] format)
        {
            DataTable dt = DataTable(builder, connectionString, timeoutSeconds, format);
            return List<T>(builder, dt, allowPrivateProperties, enforceTypesafety);
        }

        #endregion

        #region Dictionary<TKey, TValue> Methods

        public static Dictionary<TKey, T> All<TKey, T>(string keyPropertyName, string tableName = null, int? top = null, bool distinct = false, string connectionString = null, int timeoutSeconds = 30, bool allowPrivateProperties = false, bool enforceTypesafety = true)
        {
            return Dictionary<TKey, T>(null, keyPropertyName, All<T>(tableName, top, distinct, connectionString, timeoutSeconds), allowPrivateProperties, enforceTypesafety);
        }

        public static Dictionary<TKey, T> Dictionary<TKey, T>(string keyPropertyName, string tableName = null, string[] properties = null, string[] excludeProperties = null, int? top = null, bool distinct = false, string connectionString = null, int timeoutSeconds = 30, bool allowPrivateProperties = false, bool enforceTypesafety = true, params object[] format)
        {
            SqlBuilder builder = TypeBuilder.Select<T>(tableName, properties, excludeProperties, top, distinct);
            return builder.Dictionary<TKey, T>(keyPropertyName, connectionString, timeoutSeconds, allowPrivateProperties, enforceTypesafety, format);
        }

        public static TS Dictionary<TKey, T, TS>(this SqlBuilder builder, string keyPropertyName, DataTable dataTable, bool allowPrivateProperties, bool enforceTypesafety, Func<TS, TKey, T, bool> insertUpdateDelegate = null) where TS : IDictionary<TKey, T>
        {
            IDictionary<TKey, T> dict = Activator.CreateInstance<TS>() as IDictionary<TKey, T>;
            foreach (DataRow row in dataTable.Rows)
            {
                T instance = TypeBuilder.PopulateObject<T>(dataTable, row, allowPrivateProperties, enforceTypesafety);
                PropertyInfo prop = instance.GetType().GetProperty(keyPropertyName);
                if (prop != null)
                {
                    if (insertUpdateDelegate != null)
                    {
                        if (!insertUpdateDelegate((TS)dict, (TKey)prop.GetValue(instance, null), instance))
                        {
                            throw new InvalidOperationException("The InsertUpdate delegate failed to insert or update the dictionary " + typeof(TS).Name);
                        }
                    }
                    else
                    {
                        dict.Add((TKey)prop.GetValue(instance, null), instance);
                    }
                }
                else
                {
                    FieldInfo field = instance.GetType().GetField(keyPropertyName);
                    if (insertUpdateDelegate != null)
                    {
                        if (!insertUpdateDelegate((TS)dict, (TKey)field.GetValue(instance), instance))
                        {
                            throw new InvalidOperationException("The InsertUpdate delegate failed to insert or update the dictionary " + typeof(TS).Name);
                        }
                    }
                    else
                    {
                        dict.Add((TKey)field.GetValue(instance), instance);
                    }

                }
            }
            return (TS)dict;
        }

        public static Dictionary<TKey, T> Dictionary<TKey, T>(this SqlBuilder builder, string keyPropertyName, DataTable dataTable, bool allowPrivateProperties, bool enforceTypesafety)
        {
            Dictionary<TKey, T> dict = new Dictionary<TKey, T>();
            foreach (DataRow row in dataTable.Rows)
            {
                T instance = TypeBuilder.PopulateObject<T>(dataTable, row, allowPrivateProperties, enforceTypesafety);
                PropertyInfo prop = instance.GetType().GetProperty(keyPropertyName);
                if (prop != null)
                {
                    dict.Add((TKey)prop.GetValue(instance, null), instance);
                }
                else
                {
                    FieldInfo field = instance.GetType().GetField(keyPropertyName);
                    dict.Add((TKey)field.GetValue(instance), instance);
                }
            }
            return dict;
        }

        public static Dictionary<TKey, T> Dictionary<TKey, T>(this SqlBuilder builder, string keyPropertyName, string connectionString = null, int timeoutSeconds = 30, bool allowPrivateProperties = false, bool enforceTypesafety = true, params object[] format)
        {
            // Dictionary<TKey, T> dict = new Dictionary<TKey, T>();
            DataTable dt = DataTable(builder, connectionString, timeoutSeconds, format);
            return Dictionary<TKey, T, Dictionary<TKey, T>>(builder, keyPropertyName, dt, allowPrivateProperties, enforceTypesafety);
        }

        public static TS Dictionary<TKey, T, TS>(this SqlBuilder builder, string keyPropertyName, string connectionString = null, int timeoutSeconds = 30, bool allowPrivateProperties = false, bool enforceTypesafety = true, params object[] format) where TS : IDictionary<TKey, T>
        {
            IDictionary<TKey, T> dict = Activator.CreateInstance<TS>() as IDictionary<TKey, T>;
            DataTable dt = DataTable(builder, connectionString, timeoutSeconds, format);
            return Dictionary<TKey, T, TS>(builder, keyPropertyName, dt, allowPrivateProperties, enforceTypesafety);
        }

        #endregion


        public static DataTable All<T>(string tableName = null, int? top = null, bool distinct = false, string connectionString = null, int timeoutSeconds = 30)
        {
            SqlBuilder builder = TypeBuilder.Select<T>(tableName, new string[] { "*" }, null, top, distinct);
            return builder.DataTable(connectionString, timeoutSeconds, null);
        }



    }
}
