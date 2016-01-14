using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using TinySql.Metadata;
using System.Data;

namespace TinySql.UI
{
    public sealed class ListFactory
    {
        #region ctor

        private static ListFactory instance = null;
        public static ListFactory Default
        {
            get
            {
                if (instance == null)
                {
                    instance = new ListFactory();
                }
                return instance;
            }
        }

        #endregion

        #region caches

        private ConcurrentDictionary<string, ListBuilder> _PrimaryLists = new ConcurrentDictionary<string, ListBuilder>();
        public ConcurrentDictionary<string, ListBuilder> PrimaryLists
        {
            get { return _PrimaryLists; }
            set { _PrimaryLists = value; }
        }
        private ConcurrentDictionary<string, ListBuilder> _SecondaryLists = new ConcurrentDictionary<string, ListBuilder>();

        public ConcurrentDictionary<string, ListBuilder> SecondaryLists
        {
            get { return _SecondaryLists; }
            set { _SecondaryLists = value; }
        }
        private ConcurrentDictionary<string, ListBuilder> _IntegratedLists = new ConcurrentDictionary<string, ListBuilder>();

        public ConcurrentDictionary<string, ListBuilder> IntegratedLists
        {
            get { return _IntegratedLists; }
            set { _IntegratedLists = value; }
        }
        private ConcurrentDictionary<string, ListBuilder> _LookupLists = new ConcurrentDictionary<string, ListBuilder>();

        public ConcurrentDictionary<string, ListBuilder> LookupLists
        {
            get { return _LookupLists; }
            set { _LookupLists = value; }
        }
        private ConcurrentDictionary<string, List<ListBuilder>> _CustomLists = new ConcurrentDictionary<string, List<ListBuilder>>();

        public ConcurrentDictionary<string, List<ListBuilder>> CustomLists
        {
            get { return _CustomLists; }
            set { _CustomLists = value; }
        }

        public bool ToCache(ListBuilder list, ListTypes ListType, string CustomListName = null)
        {
            switch (ListType)
            {
                case ListTypes.Primary:
                    return PrimaryLists.TryAdd(list.TableName, list);

                case ListTypes.Secondary:
                    return SecondaryLists.TryAdd(list.TableName, list);

                case ListTypes.Integrated:
                    return IntegratedLists.TryAdd(list.TableName, list);

                case ListTypes.Lookup:
                    return LookupLists.TryAdd(list.TableName, list);

                case ListTypes.Custom:
                    if (string.IsNullOrEmpty(CustomListName))
                    {
                        throw new ArgumentException("Custom List name must be specified to load a custom list", "CustomListName");
                    }
                    List<ListBuilder> custom = null;
                    if (CustomLists.TryGetValue(list.TableName, out custom))
                    {
                        if (custom.Any(x => x.CustomName.Equals(CustomListName, StringComparison.OrdinalIgnoreCase)))
                        {
                            return false;
                        }
                        custom.Add(list);
                        return true;
                    }
                    else
                    {
                        return false;
                    }
                default:
                    return false;
            }
        }


        #endregion

        #region instance methods

        private void BuildTable(ListBuilder list, Table table)
        {

            foreach (Field f in table.FieldList)
            {

            }


        }

        public ListBuilder BuildList(string TableName, ListTypes ListType, string ListTitle = null, string CustomListName = null)
        {
            ListBuilder list = null;
            bool InCache = false;
            MetadataTable Table = SqlBuilder.DefaultMetadata.FindTable(TableName);
            if (Table == null)
            {
                throw new ArgumentException(string.Format("The table '{0}' was not found in metadata", TableName), "TableName");
            }

            switch (ListType)
            {
                case ListTypes.Primary:
                    InCache = PrimaryLists.TryGetValue(Table.Fullname, out list);
                    break;
                case ListTypes.Secondary:
                    InCache = SecondaryLists.TryGetValue(Table.Fullname, out list);
                    break;
                case ListTypes.Integrated:
                    InCache = IntegratedLists.TryGetValue(Table.Fullname, out list);
                    break;
                case ListTypes.Lookup:
                    InCache = LookupLists.TryGetValue(Table.Fullname, out list);
                    break;
                case ListTypes.Custom:
                    if (string.IsNullOrEmpty(CustomListName))
                    {
                        throw new ArgumentException("Custom List name must be specified to load a custom list", "CustomListName");
                    }
                    List<ListBuilder> custom = null;
                    if (CustomLists.TryGetValue(Table.Fullname, out custom))
                    {
                        InCache = custom.Any(x => x.CustomName.Equals(CustomListName, StringComparison.OrdinalIgnoreCase));
                        if (InCache)
                        {
                            list = custom.First(x => x.CustomName.Equals(CustomListName, StringComparison.OrdinalIgnoreCase));
                        }
                    }
                    break;
                default:
                    break;
            }

            if (list != null)
            {
                return list;
            }

            list = new ListBuilder()
            {
                ListType = ListType,
                TableName = Table.Fullname,
                Title = string.IsNullOrEmpty(ListTitle) ? Table.DisplayName : ListTitle,
                CustomName = CustomListName,
            };
            list.AllowNew = list.AllowEdit = list.AllowDelete = ListType != ListTypes.Lookup;
            string ListName = ListType != ListTypes.Custom ? ListType.ToString() : CustomListName;

            List<MetadataColumn> columns = new List<MetadataColumn>(Table.PrimaryKey.Columns);
            List<string> columnDef = null;
            if (!Table.ListDefinitions.TryGetValue(ListName, out columnDef))
            {
                // No standard specified
                columnDef = new List<string>(Table.Columns.Values.Where(x => x.IsPrimaryKey == false && x.IsRowGuid == false).Select(x => x.Name));
            }

            foreach (MetadataColumn column in columns)
            {
                list.Columns.Add(new ListColumn()
                {
                    ColumnName = column.Name,
                    DisplayName = column.DisplayName,
                    IsVisible = true,
                    ColumnDataType = ListColumn.GetColumnDataType(column.SqlDataType)

                });
            }

            list.Builder = Table.ToSqlBuilder(list.ListType != ListTypes.Custom ? list.ListType.ToString() : CustomListName);
            foreach (string cdef in columnDef)
            {
                MetadataColumn mc;
                SqlDbType type = SqlDbType.NVarChar;
                string Display = cdef;
                string ColName = cdef;
                if (cdef.IndexOf('=') > 0)
                {
                    ColName = cdef.Split('=')[0];
                    Display = cdef.Split('=')[1];
                }
                if (!Table.Columns.TryGetValue(ColName, out mc))
                {
                    Field f = list.Builder.Tables.SelectMany(x => x.FieldList).FirstOrDefault(x => x.Alias != null && x.Alias.Equals(ColName));
                    if (f != null)
                    {
                        type = f.SqlDataType;
                        //MetadataTable mtRelated = SqlBuilder.DefaultMetadata.FindTable((f.Table.Schema != null ? f.Table.Schema + "." : "") + f.Table.Name);
                        //MetadataColumn mcRelated;
                        //if (mtRelated.Columns.TryGetValue(f.Name, out mcRelated))
                        //{
                        //    Display = mcRelated.DisplayName;
                        //}
                    }
                }
                else
                {
                    Display = mc.DisplayName;
                    ColName = mc.Name;
                    type = mc.SqlDataType;
                }
                list.Columns.Add(new ListColumn()
                {
                    ColumnName = ColName,
                    DisplayName = Display,
                    IsVisible = mc != null ? !mc.IsForeignKey : true,
                    ColumnDataType = ListColumn.GetColumnDataType(type)
                });
            }

            //columns.AddRange(
            //    Table.Columns.Values.Where(x => columnDef.Contains(x.Name))
            //    );



            ToCache(list, ListType, CustomListName);

            return list;


        }

        #endregion




    }
}
