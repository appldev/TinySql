using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.SqlServer.Management.Common;
using Microsoft.SqlServer.Management.Smo;
using System.Data;
using System.Runtime.Caching;
using TinySql.Serialization;
using System.IO;
using System.Collections.Concurrent;

namespace TinySql.Metadata
{
    public class SqlMetadataDatabase
    {
        #region ctor
        internal SqlMetadataDatabase(string ConnectionString, bool UseCache = true, string FileName = null)
        {
            Initialize(ConnectionString, UseCache, FileName);
            this.builder = new SqlBuilder(ConnectionString);
        }
        internal SqlMetadataDatabase(SqlBuilder Builder, bool UseCache = true, string FileName = null)
        {
            Initialize(Builder.ConnectionString ?? SqlBuilder.DefaultConnection, UseCache, FileName);
            this.builder = Builder;
        }

        private void Initialize(string connectionString, bool useCache, string FileName)
        {
            ConnectionBuilder = new System.Data.SqlClient.SqlConnectionStringBuilder(connectionString);
            this.UseCache = useCache;
            this.MetadataKey = ConnectionBuilder.ConnectionString.GetHashCode().ToString();
            this.FileName = FileName;

        }

        #endregion

        public delegate void MetadataUpdateDelegate(int PercentDone, string Message, DateTime timestamp);

        public event MetadataUpdateDelegate MetadataUpdateEvent;

        private List<string> TablesInProgress = new List<string>();

        public static SqlMetadataDatabase FromBuilder(SqlBuilder Builder, bool UseCache = true, string FileName = null)
        {
            return new SqlMetadataDatabase(Builder, UseCache, FileName);
        }
        public static SqlMetadataDatabase FromConnection(string ConnectionString, bool UseCache = true, string FileName = null)
        {
            return new SqlMetadataDatabase(ConnectionString, UseCache, FileName);
        }

        private void RaiseUpdateEvent(int PercentDone, string Message)
        {
            if (MetadataUpdateEvent == null)
            {
                return;
            }
            else
            {
                MetadataUpdateEvent.Invoke(PercentDone, Message, DateTime.Now);
            }
        }

        private void RetrieveExtendedProperties(ExtendedPropertyCollection properties, SqlExtendedPropertyContainer container)
        {
            properties.Refresh();
            foreach (ExtendedProperty prop in properties)
            {
                prop.Refresh();
                container.Properties.TryAdd(prop.Name, prop.Value.ToString());
            }
        }


        private bool UpdateExtendedProperties(ExtendedPropertyCollection properties, SqlExtendedPropertyContainer container)
        {
            bool DoAlter = false;
            ExtendedProperty prop = null;
            foreach (string key in container.Properties.Keys)
            {
                if (properties.Contains(key))
                {
                    prop = properties[key];
                }
                else
                {
                    prop = new ExtendedProperty(properties.Parent, key);
                    properties.Add(prop);
                    DoAlter = true;
                }
                prop.Value = container.Properties[key];
                prop.Alter();
            }

            foreach (ExtendedProperty prop2 in properties)
            {
                if (!container.Properties.ContainsKey(prop2.Name))
                {
                    prop2.MarkForDrop(true);
                    DoAlter = true;
                }
            }
            return DoAlter;

        }
        public void ImportExtendedProperties(DatabaseExtendedProperties dep)
        {
            double t = 0;
            SqlDatabase.Refresh();
            double total = dep.Tables.Count;
            // bool DoAlter = UpdateExtendedProperties(SqlDatabase.ExtendedProperties,)
            RaiseUpdateEvent(0, "Database level extended properties extported");
            foreach (Microsoft.SqlServer.Management.Smo.Table table in SqlDatabase.Tables)
            {
                TableExtendedProperties tep = dep.Tables.FirstOrDefault(x => x.Name.Equals(table.Name, StringComparison.OrdinalIgnoreCase) && x.Schema.Equals(table.Schema, StringComparison.OrdinalIgnoreCase));
                if (tep != null)
                {
                    table.Refresh();
                    bool alter = UpdateExtendedProperties(table.ExtendedProperties, tep);
                    if (tep.Columns.Count > 0)
                    {
                        foreach (Column column in table.Columns)
                        {
                            SqlExtendedPropertyContainer cep = tep.Columns.FirstOrDefault(x => x.Name.Equals(column.Name));
                            if (cep != null)
                            {
                                if (UpdateExtendedProperties(column.ExtendedProperties, cep))
                                {
                                    column.Alter();
                                }
                            }
                        }
                    }
                    if (alter)
                    {
                        table.Alter();
                    }
                    t++;
                    RaiseUpdateEvent(Convert.ToInt32((t / total) * 100), table.Schema + "." + table.Name + " extended properties imported");
                }
            }

        }
        public DatabaseExtendedProperties ExportExtendedProperties()
        {
            DatabaseExtendedProperties dep = new DatabaseExtendedProperties();
            double t = 0;
            SqlDatabase.Refresh();
            double total = SqlDatabase.Tables.Count;
            RetrieveExtendedProperties(SqlDatabase.ExtendedProperties, dep);
            RaiseUpdateEvent(0, "Database level extended properties extported");
            foreach (Microsoft.SqlServer.Management.Smo.Table table in SqlDatabase.Tables)
            {
                table.Refresh();
                TableExtendedProperties tep = new TableExtendedProperties();
                RetrieveExtendedProperties(table.ExtendedProperties, tep);
                tep.Schema = table.Schema;
                tep.Name = table.Schema;
                foreach (Column column in table.Columns)
                {
                    SqlExtendedPropertyContainer cep = new SqlExtendedPropertyContainer();
                    RetrieveExtendedProperties(column.ExtendedProperties, cep);
                    if (cep.Properties.Count > 0)
                    {
                        cep.Schema = column.DefaultSchema;
                        cep.Name = column.Name;
                        tep.Columns.Add(cep);
                    }
                }
                if (tep.Properties.Count > 0 || tep.Columns.Count > 0)
                {
                    dep.Tables.Add(tep);
                }
                t++;
                RaiseUpdateEvent(Convert.ToInt32((t / total) * 100), table.Schema + "." + table.Name + " extended properties exported");
            }

            return dep;
        }

        public MetadataDatabase BuildMetadata(bool PrimaryKeyIndexOnly = true, string[] Tables = null, bool UpdateExisting = false)
        {
            MetadataDatabase mdb = FromCache();
            string[] Changes = null;
            if (mdb != null && !UpdateExisting)
            {
                // mdb.Builder = builder;
                return mdb;
            }
            Guid g = Guid.NewGuid();
            try
            {
                g = SqlDatabase.DatabaseGuid;
            }
            catch (Exception)
            {
            }

            if (UpdateExisting)
            {
                if (mdb == null)
                {
                    throw new ArgumentException("Update was specified but the metadata was not found in cache or file", "UpdateExisting");
                }
                long v = GetVersion();
                if (v <= mdb.Version)
                {
                    RaiseUpdateEvent(100, "The database is up to date");
                    return mdb;
                }
                else
                {
                    Changes = GetChanges(new DateTime(mdb.Version));
                    RaiseUpdateEvent(0, string.Format("{0} Changed tables identified", Changes.Length));
                    foreach (string change in Changes)
                    {
                        MetadataTable mt = null;

                        if (mdb.Tables.TryRemove(change, out mt))
                        {
                            RaiseUpdateEvent(0, string.Format("{0} removed from Metadata pending update", change));
                        }
                        else
                        {
                            throw new InvalidOperationException("Could not remove the table " + change + " pending update");
                        }
                    }
                    mdb.Version = v;
                }

            }
            else
            {
                mdb = new MetadataDatabase()
                {
                    Id = g,
                    Name = SqlDatabase.Name,
                    Server = SqlServer.Name + (!string.IsNullOrEmpty(SqlServer.InstanceName) && SqlServer.Name.IndexOf('\\') == -1 ? "" : ""),
                    Builder = builder,
                    Version = GetVersion()
                };
            }

            double t = 0;
            double total = Changes != null ? Changes.Length : Tables != null ? Tables.Length : SqlDatabase.Tables.Count + SqlDatabase.Views.Count;
            foreach (Microsoft.SqlServer.Management.Smo.Table table in SqlDatabase.Tables)
            {
                if (Tables == null || Tables.Contains(table.Name))
                {
                    if (Changes == null || Changes.Contains(table.Schema + "." + table.Name))
                    {
                        table.Refresh();
                        BuildMetadata(mdb, table);
                        if (MetadataUpdateEvent != null)
                        {
                            t++;
                            RaiseUpdateEvent(Convert.ToInt32((t / total) * 100), table.Schema + "." + table.Name + " built");
                        }
                        if (t == total)
                        {
                            break;
                        }
                    }
                }
            }

            foreach (Microsoft.SqlServer.Management.Smo.View view in SqlDatabase.Views)
            {
                if (view.IsSystemObject)
                {
                    t++;
                    continue;
                }
                if (Tables == null || Tables.Contains(view.Name))
                {
                    if (Changes == null ||Changes.Contains(view.Schema + "." + view.Name))
                    {
                        view.Refresh();
                        BuildMetadataView(mdb, view);
                        if (MetadataUpdateEvent != null)
                        {
                            t++;
                            RaiseUpdateEvent(Convert.ToInt32((t / total) * 100), view.Schema + "." + view.Name + " built");
                        }
                        if (t == total)
                        {
                            break;
                        }
                    }
                }
            }


            ToCache(mdb);
            return mdb;

        }

        private string GetExtendedProperty(string PropertyName, ExtendedPropertyCollection props)
        {
            if (props.Count > 0)
            {
                string key = "TinySql." + PropertyName;
                if (props.Contains(key))
                {
                    return Convert.ToString(props[key].Value);
                }
            }
            return null;
        }

        private string[] GetExtendedProperty(string PropertyName, ExtendedPropertyCollection props, char[] separators)
        {
            string value = GetExtendedProperty(PropertyName, props);
            if (value != null)
            {
                return value.Split(separators, StringSplitOptions.RemoveEmptyEntries);
            }
            return null;
        }

        private MetadataTable BuildMetadataView(MetadataDatabase mdb, Microsoft.SqlServer.Management.Smo.View view, bool PrimaryKeyIndexOnly = true, bool SelfJoin = false)
        {
            MetadataTable mt = null;
            List<VirtualForeignKey> VirtualKeys = new List<VirtualForeignKey>();
            view.Refresh();
            if (mdb.Tables.TryGetValue(view.Name, out mt))
            {
                return mt;
            }

            mt = new MetadataTable()
            {
                Id = view.ID,
                Schema = view.Schema,
                Name = view.Name,
                //Parent = mdb
            };
            mt.TitleColumn = GetExtendedProperty("TitleColumn", view.ExtendedProperties);
            string[] values = GetExtendedProperty("DisplayName", view.ExtendedProperties, new char[] { '\r', '\n' });
            if (values != null)
            {
                foreach (string value in values)
                {
                    string[] v = value.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                    mt.DisplayNames.TryAdd(Convert.ToInt32(v[0]), v[1]);
                }
            }

            values = GetExtendedProperty("Lists", view.ExtendedProperties, new char[] { '\r', '\n' });
            if (values != null)
            {
                foreach (string value in values)
                {
                    string[] v = value.Split(new char[] { ':' }, StringSplitOptions.RemoveEmptyEntries);
                    List<string> v2 = v[1].Split(',').ToList();
                    if (!mt.ListDefinitions.TryAdd(v[0].Trim(), v2))
                    {
                        throw new InvalidOperationException(string.Format("The TinySql.Lists extended property is invalid for the table '{0}'", view.Name));
                    }
                }
            }






            foreach (Microsoft.SqlServer.Management.Smo.Column column in view.Columns)
            {
                try
                {
                    MetadataColumn col = new MetadataColumn()
                    {
                        Id = column.ID,
                        Parent = mt,
                        //Database = mdb,
                        Name = column.Name,
                        Collation = column.Collation,
                        Default = column.Default,
                        IsComputed = column.Computed,
                        ComputedText = column.ComputedText,
                        IsPrimaryKey = column.InPrimaryKey,
                        IsIdentity = column.Identity,
                        IsForeignKey = column.IsForeignKey,
                        IdentityIncrement = column.IdentityIncrement,
                        IdentitySeed = column.IdentitySeed,
                        Nullable = column.Nullable,
                        IsRowGuid = column.RowGuidCol
                    };
                    BuildColumnDataType(col, column);

                    values = GetExtendedProperty("DisplayName", column.ExtendedProperties, new char[] { '\r', '\n' });
                    if (values != null)
                    {
                        foreach (string value in values)
                        {
                            if (!value.Contains("="))
                            {
                                col.DisplayNames.TryAdd(SqlBuilder.DefaultCulture.LCID, value);
                            }
                            else
                            {
                                string[] v = value.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                                col.DisplayNames.TryAdd(Convert.ToInt32(v[0]), v[1]);
                            }

                        }
                    }





                    col.IncludeColumns = GetExtendedProperty("IncludeColumns", column.ExtendedProperties, new char[] { ',' });

                    values = GetExtendedProperty("FK", column.ExtendedProperties, new char[] { '\r', '\n' });
                    if (values != null)
                    {
                        VirtualKeys.Add(new VirtualForeignKey() { Column = col, values = values });
                        col.IsForeignKey = true;
                    }



                    mt.Columns.AddOrUpdate(col.Name, col, (k, v) => { return col; });


                }
                catch (Exception exColumn)
                {
                    throw new InvalidOperationException(string.Format("Unable to generate the column {0}", column.Name), exColumn);
                }
            }


            if (view.IsIndexable)
            {
                foreach (Index idx in view.Indexes)
                {
                    if (!PrimaryKeyIndexOnly || idx.IndexKeyType == IndexKeyType.DriPrimaryKey)
                    {
                        Key key = new Key()
                        {
                            Id = idx.ID,
                            Parent = mt,
                            Database = mdb,
                            Name = idx.Name,
                            IsUnique = idx.IsUnique,
                            IsPrimaryKey = idx.IndexKeyType == IndexKeyType.DriPrimaryKey
                        };
                        foreach (IndexedColumn c in idx.IndexedColumns)
                        {
                            key.Columns.Add(mt[c.Name]);
                        }
                        mt.Indexes.AddOrUpdate(key.Name, key, (k, v) => { return key; });
                    }
                }
            }
            
            //if (!SelfJoin)
            //{
            //    foreach (ForeignKey FK in view.ForeignKeys)
            //    {
            //        MetadataForeignKey mfk = new MetadataForeignKey()
            //        {
            //            ID = FK.ID,
            //            Parent = mt,
            //            Database = mdb,
            //            Name = FK.Name,
            //            ReferencedKey = FK.ReferencedKey,
            //            ReferencedSchema = FK.ReferencedTableSchema,
            //            ReferencedTable = FK.ReferencedTable
            //        };
            //        MetadataTable mtref = null;
            //        if (!mdb.Tables.TryGetValue(mfk.ReferencedSchema + "." + mfk.ReferencedTable, out mtref))
            //        {
            //            bool self = false;
            //            if ((mfk.ReferencedSchema == mt.Schema && mfk.ReferencedTable == mt.Name) || TablesInProgress.Contains(mfk.ReferencedSchema + "." + mfk.ReferencedTable))
            //            {
            //                self = true;
            //            }
            //            TablesInProgress.Add(mfk.ReferencedSchema + "." + mfk.ReferencedTable);
            //            mtref = BuildMetadata(mdb, mfk.ReferencedTable, mfk.ReferencedSchema, PrimaryKeyIndexOnly, self);
            //        }
            //        foreach (ForeignKeyColumn cc in FK.Columns)
            //        {
            //            mfk.ColumnReferences.Add(new MetadataColumnReference()
            //            {
            //                Name = cc.Name,
            //                Column = mt[cc.Name],
            //                ReferencedColumn = mtref[cc.ReferencedColumn]
            //            });
            //        }
            //        mt.ForeignKeys.AddOrUpdate(mfk.Name, mfk, (key, existing) =>
            //        {
            //            return mfk;
            //        });
            //    }
            //}

            if (VirtualKeys.Count > 0)
            {
                BuildVirtualKeys(VirtualKeys, mt, mdb, PrimaryKeyIndexOnly);
            }

            mdb.Tables.AddOrUpdate(mt.Schema + "." + mt.Name, mt, (key, existing) =>
            {
                return mt;
            });
            return mt;
        }

        private MetadataTable BuildMetadata(MetadataDatabase mdb, Microsoft.SqlServer.Management.Smo.Table table, bool PrimaryKeyIndexOnly = true, bool SelfJoin = false)
        {
            MetadataTable mt = null;
            List<VirtualForeignKey> VirtualKeys = new List<VirtualForeignKey>();
            table.Refresh();
            if (mdb.Tables.TryGetValue(table.Name, out mt))
            {
                return mt;
            }

            mt = new MetadataTable()
            {
                Id = table.ID,
                Schema = table.Schema,
                Name = table.Name,
                //Parent = mdb
            };
            mt.TitleColumn = GetExtendedProperty("TitleColumn", table.ExtendedProperties);
            string[] values = GetExtendedProperty("DisplayName", table.ExtendedProperties, new char[] { '\r', '\n' });
            if (values != null)
            {
                foreach (string value in values)
                {
                    string[] v = value.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                    mt.DisplayNames.TryAdd(Convert.ToInt32(v[0]), v[1]);
                }
            }

            values = GetExtendedProperty("Lists", table.ExtendedProperties, new char[] { '\r', '\n' });
            if (values != null)
            {
                foreach (string value in values)
                {
                    string[] v = value.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                    List<string> v2 = v[1].Split(',').ToList();
                    if (!mt.ListDefinitions.TryAdd(v[0].Trim(), v2))
                    {
                        throw new InvalidOperationException(string.Format("The TinySql.Lists extended property is invalid for the table '{0}'", table.Name));
                    }
                }
            }






            foreach (Microsoft.SqlServer.Management.Smo.Column column in table.Columns)
            {
                try
                {
                    MetadataColumn col = new MetadataColumn()
                    {
                        Id = column.ID,
                        Parent = mt,
                        //Database = mdb,
                        Name = column.Name,
                        Collation = column.Collation,
                        Default = column.Default,
                        IsComputed = column.Computed,
                        ComputedText = column.ComputedText,
                        IsPrimaryKey = column.InPrimaryKey,
                        IsIdentity = column.Identity,
                        IsForeignKey = column.IsForeignKey,
                        IdentityIncrement = column.IdentityIncrement,
                        IdentitySeed = column.IdentitySeed,
                        Nullable = column.Nullable,
                        IsRowGuid = column.RowGuidCol
                    };
                    BuildColumnDataType(col, column);

                    values = GetExtendedProperty("DisplayName", column.ExtendedProperties, new char[] { '\r', '\n' });
                    if (values != null)
                    {
                        foreach (string value in values)
                        {
                            if (!value.Contains("="))
                            {
                                col.DisplayNames.TryAdd(SqlBuilder.DefaultCulture.LCID, value);
                            }
                            else
                            {
                                string[] v = value.Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries);
                                col.DisplayNames.TryAdd(Convert.ToInt32(v[0]), v[1]);
                            }

                        }
                    }





                    col.IncludeColumns = GetExtendedProperty("IncludeColumns", column.ExtendedProperties, new char[] { ',' });

                    values = GetExtendedProperty("FK", column.ExtendedProperties, new char[] { '\r', '\n' });
                    if (values != null)
                    {
                        VirtualKeys.Add(new VirtualForeignKey() { Column = col, values = values });
                        col.IsForeignKey = true;
                    }



                    mt.Columns.AddOrUpdate(col.Name, col, (k, v) => { return col; });


                }
                catch (Exception exColumn)
                {
                    throw new InvalidOperationException(string.Format("Unable to generate the column {0}", column.Name), exColumn);
                }
            }



            foreach (Index idx in table.Indexes)
            {
                if (!PrimaryKeyIndexOnly || idx.IndexKeyType == IndexKeyType.DriPrimaryKey)
                {
                    Key key = new Key()
                    {
                        Id = idx.ID,
                        Parent = mt,
                        Database = mdb,
                        Name = idx.Name,
                        IsUnique = idx.IsUnique,
                        IsPrimaryKey = idx.IndexKeyType == IndexKeyType.DriPrimaryKey
                    };
                    foreach (IndexedColumn c in idx.IndexedColumns)
                    {
                        key.Columns.Add(mt[c.Name]);
                    }
                    mt.Indexes.AddOrUpdate(key.Name, key, (k, v) => { return key; });
                }
            }
            if (!SelfJoin)
            {
                foreach (ForeignKey FK in table.ForeignKeys)
                {
                    MetadataForeignKey mfk = new MetadataForeignKey()
                    {
                        Id = FK.ID,
                        Parent = mt,
                        Database = mdb,
                        Name = FK.Name,
                        ReferencedKey = FK.ReferencedKey,
                        ReferencedSchema = FK.ReferencedTableSchema,
                        ReferencedTable = FK.ReferencedTable
                    };
                    MetadataTable mtref = null;
                    if (!mdb.Tables.TryGetValue(mfk.ReferencedSchema + "." + mfk.ReferencedTable, out mtref))
                    {
                        bool self = false;
                        if ((mfk.ReferencedSchema == mt.Schema && mfk.ReferencedTable == mt.Name) || TablesInProgress.Contains(mfk.ReferencedSchema + "." + mfk.ReferencedTable))
                        {
                            self = true;
                        }
                        TablesInProgress.Add(mfk.ReferencedSchema + "." + mfk.ReferencedTable);
                        mtref = BuildMetadata(mdb, mfk.ReferencedTable, mfk.ReferencedSchema, PrimaryKeyIndexOnly, self);
                    }
                    foreach (ForeignKeyColumn cc in FK.Columns)
                    {
                        mfk.ColumnReferences.Add(new MetadataColumnReference()
                        {
                            Name = cc.Name,
                            Column = mt[cc.Name],
                            ReferencedColumn = mtref[cc.ReferencedColumn]
                        });
                    }
                    mt.ForeignKeys.AddOrUpdate(mfk.Name, mfk, (key, existing) =>
                    {
                        return mfk;
                    });
                }
            }

            if (VirtualKeys.Count > 0)
            {
                BuildVirtualKeys(VirtualKeys, mt, mdb, PrimaryKeyIndexOnly);
            }

            mdb.Tables.AddOrUpdate(mt.Schema + "." + mt.Name, mt, (key, existing) =>
            {
                return mt;
            });
            return mt;
        }

        private void BuildVirtualKeys(List<VirtualForeignKey> keys, MetadataTable mt, MetadataDatabase mdb, bool PrimaryKeyIndexOnly)
        {
            foreach (VirtualForeignKey vfk in keys)
            {
                MetadataForeignKey mfk = new MetadataForeignKey()
                {
                    Id = 0,
                    Name = vfk.values[0].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries)[0],
                    ReferencedKey = "",
                    ReferencedTable = vfk.values[0].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries)[1],
                    ReferencedSchema = vfk.values[0].Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries)[2],
                    Parent = mt,
                    IsVirtual = true
                };
                MetadataTable mtref = null;
                if (!mdb.Tables.TryGetValue(mfk.ReferencedSchema + "." + mfk.ReferencedTable, out mtref))
                {
                    bool self = false;
                    if (mfk.ReferencedSchema == mt.Schema && mfk.ReferencedTable == mt.Name)
                    {
                        self = true;
                    }
                    mtref = BuildMetadata(mdb, mfk.ReferencedTable, mfk.ReferencedSchema, PrimaryKeyIndexOnly, self);
                }
                for (int i = 1; i < vfk.values.Length; i++)
                {
                    MetadataColumnReference mcf = new MetadataColumnReference()
                    {
                        ReferencedColumn = mtref[vfk.values[i].Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries)[1]],
                    };
                    string from = vfk.values[i].Split(new char[] { '=' }, StringSplitOptions.RemoveEmptyEntries)[0];
                    if (from.StartsWith("\""))
                    {
                        MetadataColumn mcVirtual = new MetadataColumn()
                        {
                            Name = from,
                            IsForeignKey = true,
                            Id = 0,
                            SqlDataType = SqlDbType.NVarChar,
                            Nullable = false,
                            Length = 0,
                            IsComputed = true,
                            DataType = typeof(string),
                        };
                        mcf.Column = mcVirtual;
                        mcf.Name = from;
                    }
                    else
                    {
                        mcf.Column = mt[from];
                        mcf.Name = mcf.Column.Name;
                    }
                    mfk.ColumnReferences.Add(mcf);
                }
                mt.ForeignKeys.AddOrUpdate(mfk.Name, mfk, (k, v) => { return mfk; });
            }
        }

        public MetadataTable BuildMetadata(MetadataDatabase mdb, string TableName, string Schema = "dbo", bool PrimaryKeyIndexOnly = true, bool SelfJoin = false)
        {
            MetadataTable mt = null;
            if (mdb.Tables.TryGetValue(TableName, out mt))
            {
                return mt;
            }
            Microsoft.SqlServer.Management.Smo.Table t = new Microsoft.SqlServer.Management.Smo.Table(SqlDatabase, TableName, Schema);
            t.Refresh();
            return BuildMetadata(mdb, t, PrimaryKeyIndexOnly, SelfJoin);
        }


        private string[] GetChanges(DateTime FromDate)
        {
            string sql = string.Format(CHANGE_SQL, FromDate.ToString("s"));
            DataSet ds = SqlDatabase.ExecuteWithResults(sql);
            List<string> Tables = new List<string>();
            if (ds.Tables.Count == 1)
            {
                foreach (DataRow dr in ds.Tables[0].Rows)
                {
                    if (!dr.IsNull(0) && !dr.IsNull(1))
                    {
                        Tables.Add(dr.Field<string>(0) + "." + dr.Field<string>(1));
                    }
                }
            }
            return Tables.ToArray();
        }

        private long GetVersion()
        {
            try
            {
                DataSet ds = SqlDatabase.ExecuteWithResults(VERSION_SQL);
                if (ds.Tables.Count == 1 && ds.Tables[0].Rows.Count == 1 && !ds.Tables[0].Rows[0].IsNull(0))
                {
                    DateTime date = ds.Tables[0].Rows[0].Field<DateTime>(0);
                    return date.Ticks;
                }
                return 0;
            }
            catch (Exception)
            {
                return -1;
            }
        }

        private const string CHANGE_SQL = "select ss.name,so.name from sys.objects so inner join sys.schemas ss on (so.[schema_id] = ss.[schema_id]) where so.is_ms_shipped = 0 AND so.type = N'U' AND so.modify_date > '{0}'";
        private const string VERSION_SQL = "select MAX(modify_date) from sys.objects where is_ms_shipped = 0 AND type = N'U'";

        private bool ToCache(MetadataDatabase mdb)
        {
            if (!UseCache)
            {
                return true;
            }
            return CacheMetadata(MetadataKey, mdb);
        }

        private MetadataDatabase FromCache()
        {
            if (UseCache == false)
            {
                return null;
            }
            MetadataDatabase mdb = CacheMetadata(MetadataKey);
            if (mdb != null)
            {
                return mdb;
            }
            if (!string.IsNullOrEmpty(FileName))
            {
                mdb = SerializationExtensions.FromFile(FileName);
                CacheMetadata(MetadataKey, mdb);
            }
            return mdb;
        }

        private static CacheItemPolicy _CachePolicy = new CacheItemPolicy() { AbsoluteExpiration = MemoryCache.InfiniteAbsoluteExpiration, SlidingExpiration = MemoryCache.NoSlidingExpiration };
        public static CacheItemPolicy CachePolicy
        {
            get { return _CachePolicy; }
            set { _CachePolicy = value; }
        }


        public static bool CacheMetadata(string MetadataKey, MetadataDatabase mdb, CacheItemPolicy Policy = null)
        {
            if (MemoryCache.Default.Contains(MetadataKey))
            {
                return true;
            }
            CacheItem item = MemoryCache.Default.AddOrGetExisting(new CacheItem(MetadataKey, SerializationExtensions.ToJson<MetadataDatabase>(mdb)), (Policy ?? CachePolicy));
            return item.Value == null;
        }

        public bool ClearMetadata()
        {
            return MemoryCache.Default.Remove(MetadataKey) != null;
        }


        public static MetadataDatabase CacheMetadata(string MetadataKey)
        {
            CacheItem item = MemoryCache.Default.GetCacheItem(MetadataKey);
            if (item != null)
            {
                return SerializationExtensions.FromJson<MetadataDatabase>(item.Value.ToString());
            }
            return null;
        }






        private void BuildColumnDataType(MetadataColumn column, Column col)
        {
            DataType ColumnType = col.DataType;

            switch (ColumnType.SqlDataType)
            {
                case SqlDataType.UserDefinedDataType:
                    SqlDatabase.UserDefinedDataTypes.Refresh(true);
                    UserDefinedDataType udt = SqlDatabase.UserDefinedDataTypes[ColumnType.Name];
                    column.SqlDataType = (SqlDbType)Enum.Parse(typeof(SqlDbType), udt.SystemType, true);
                    column.Length = udt.Length;
                    column.Scale = ColumnType.NumericScale;
                    column.Precision = ColumnType.NumericPrecision;
                    break;

                case SqlDataType.SysName:
                    column.SqlDataType = SqlDbType.NVarChar;
                    column.Length = 128;
                    break;

                case SqlDataType.UserDefinedType:
                    column.SqlDataType = System.Data.SqlDbType.Udt;
                    column.Scale = ColumnType.NumericScale;
                    column.Precision = ColumnType.NumericPrecision;
                    break;

                case SqlDataType.NVarCharMax:
                    column.SqlDataType = System.Data.SqlDbType.NVarChar;
                    break;

                case SqlDataType.VarCharMax:
                    column.SqlDataType = System.Data.SqlDbType.VarChar;
                    break;

                case SqlDataType.VarBinaryMax:
                    column.SqlDataType = SqlDbType.VarBinary;
                    break;

                case SqlDataType.Numeric:
                    column.SqlDataType = SqlDbType.Decimal;
                    column.Scale = ColumnType.NumericScale;
                    column.Precision = ColumnType.NumericPrecision;
                    break;

                case SqlDataType.UserDefinedTableType:
                case SqlDataType.HierarchyId:
                case SqlDataType.Geometry:
                case SqlDataType.Geography:
                    column.SqlDataType = SqlDbType.Variant;
                    column.Scale = ColumnType.NumericScale;
                    column.Precision = ColumnType.NumericPrecision;
                    break;

                default:
                    column.SqlDataType = (SqlDbType)Enum.Parse(typeof(SqlDbType), ColumnType.SqlDataType.ToString());
                    switch (column.SqlDataType)
                    {
                        case SqlDbType.Decimal:
                        case SqlDbType.Float:
                        case SqlDbType.Money:
                        case SqlDbType.Real:
                        case SqlDbType.SmallMoney:
                            column.Scale = ColumnType.NumericScale;
                            column.Precision = ColumnType.NumericPrecision;
                            break;

                        default:
                            break;
                    }
                    break;
            }

            column.Length = ColumnType.MaximumLength;
            column.DataType = ConvertSqlTypeToType(column.SqlDataType);
        }

        private static Type ConvertSqlTypeToType(SqlDbType sqlDataType)
        {
            switch (sqlDataType)
            {
                case SqlDbType.BigInt:
                    return typeof(Int64);

                case SqlDbType.Binary:
                case SqlDbType.Image:
                case SqlDbType.Timestamp:
                case SqlDbType.VarBinary:
                    return typeof(Byte[]);

                case SqlDbType.Bit:
                    return typeof(Boolean);

                case SqlDbType.Char:
                case SqlDbType.NChar:
                case SqlDbType.NText:
                case SqlDbType.NVarChar:
                case SqlDbType.Text:
                case SqlDbType.VarChar:
                    return typeof(String);

                case SqlDbType.Date:
                case SqlDbType.DateTime:
                case SqlDbType.DateTime2:
                case SqlDbType.SmallDateTime:
                case SqlDbType.Time:
                    return typeof(DateTime);

                case SqlDbType.DateTimeOffset:
                    return typeof(DateTimeOffset);

                case SqlDbType.Decimal:
                case SqlDbType.Money:
                case SqlDbType.SmallMoney:
                    return typeof(Decimal);

                case SqlDbType.Float:
                    return typeof(Double);

                case SqlDbType.Int:
                    return typeof(Int32);

                case SqlDbType.Real:
                    return typeof(Single);

                case SqlDbType.SmallInt:
                    return typeof(Int16);

                case SqlDbType.TinyInt:
                    return typeof(Byte);

                case SqlDbType.UniqueIdentifier:
                    return typeof(Guid);

                case SqlDbType.Xml:
                    return typeof(System.Xml.XmlDocument);

                case SqlDbType.Structured:
                case SqlDbType.Udt:
                case SqlDbType.Variant:
                default:
                    return typeof(Object);
            }
        }


        private static ServerConnection GetConnection(SqlBuilder Builder)
        {
            return GetConnection(Builder.ConnectionString ?? SqlBuilder.DefaultConnection);
        }
        private static ServerConnection GetConnection(string ConnectionString)
        {
            System.Data.SqlClient.SqlConnection con = new System.Data.SqlClient.SqlConnection(ConnectionString);
            return new ServerConnection(con);
        }

        private System.Data.SqlClient.SqlConnectionStringBuilder _ConnectionBuilder = null;
        public System.Data.SqlClient.SqlConnectionStringBuilder ConnectionBuilder
        {
            get { return _ConnectionBuilder; }
            private set { _ConnectionBuilder = value; }
        }
        private Server _SqlServer = null;
        public Server SqlServer
        {
            get
            {
                if (_SqlServer == null)
                {
                    _SqlServer = new Server(GetConnection(ConnectionBuilder.ConnectionString));
                }
                return _SqlServer;
            }
            set { _SqlServer = value; }
        }
        private Database _SqlDatabase;

        public Database SqlDatabase
        {
            get
            {
                if (_SqlDatabase == null)
                {
                    string db = ConnectionBuilder.InitialCatalog;
                    if (!string.IsNullOrEmpty(ConnectionBuilder.AttachDBFilename))
                    {
                        db = Path.GetFileNameWithoutExtension(ConnectionBuilder.AttachDBFilename);
                    }
                    _SqlDatabase = new Database(SqlServer, db);
                    _SqlDatabase.Refresh();
                }
                return _SqlDatabase;
            }
            set { _SqlDatabase = value; }
        }
        internal SqlBuilder builder;
        public string MetadataKey { get; private set; }
        private bool _UseCache = true;

        public bool UseCache
        {
            get { return _UseCache; }
            private set { _UseCache = value; }
        }

        public string FileName { get; set; }





    }

    internal class VirtualForeignKey
    {
        public MetadataColumn Column = null;
        public string[] values = null;

    }


    public class SqlExtendedPropertyContainer
    {
        private ConcurrentDictionary<string, string> _Properties = new ConcurrentDictionary<string, string>();

        public ConcurrentDictionary<string, string> Properties
        {
            get { return _Properties; }
            set { _Properties = value; }
        }

        public string Name { get; set; }
        public string Schema { get; set; }
    }

    public class TableExtendedProperties : SqlExtendedPropertyContainer
    {
        private List<SqlExtendedPropertyContainer> _Columns = new List<SqlExtendedPropertyContainer>();

        public List<SqlExtendedPropertyContainer> Columns
        {
            get { return _Columns; }
            set { _Columns = value; }
        }
    }

    public class DatabaseExtendedProperties : SqlExtendedPropertyContainer
    {
        private List<TableExtendedProperties> _Tables = new List<TableExtendedProperties>();

        public List<TableExtendedProperties> Tables
        {
            get { return _Tables; }
            set { _Tables = value; }
        }

    }

}
