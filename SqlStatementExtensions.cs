using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using TinySql.Metadata;

namespace TinySql
{
    public class TableHelper<TModel>
    {
        public TModel Model;
        public Table Table;
    }
    public static class SqlStatementExtensions
    {
        #region Support functions
        public static Table FindTable(this SqlBuilder builder, string tableNameOrAlias, string schema = null)
        {
            Table found = builder.Tables.FirstOrDefault(x => x.Name.Equals(tableNameOrAlias, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(tableNameOrAlias, StringComparison.InvariantCultureIgnoreCase)));
            if (found != null)
            {
                return found;
            }
            else if (builder.ParentBuilder != null)
            {
                SqlBuilder b = builder.ParentBuilder;
                while (b != null && found == null)
                {
                    found = builder.Tables.FirstOrDefault(x => x.Name.Equals(tableNameOrAlias, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(tableNameOrAlias, StringComparison.InvariantCultureIgnoreCase)));
                    b = b.ParentBuilder;
                }
            }
            return found;
        }

        public static Field FindField(this Table table, string nameOrAlias)
        {
            return table.FieldList.FirstOrDefault(x => x.Name.Equals(nameOrAlias, StringComparison.OrdinalIgnoreCase) || (x.Alias != null && x.Alias.Equals(nameOrAlias, StringComparison.OrdinalIgnoreCase)));
        }

        public static Table Property<TModel, TProperty>(this TableHelper<TModel> helper, Expression<Func<TModel, TProperty>> expression)
        {
            Expression exp = expression.ReduceExtensions();
            return helper.Table;
        }


        #endregion

        #region Stored Procedures

        public static StoredProcedure Parameter(this StoredProcedure proc, string name, SqlDbType sqlDataType, object value, Type dataType, int maxLength = -1, int scale = -1, int precision = -1, bool isOutput = false, string tableName = null, string fieldName = null)
        {
            ParameterField p = new ParameterField()
            {
                Builder = proc.Builder,
                Name = name,
                ParameterName = "@" + name,
                MaxLength = maxLength,
                Scale = scale,
                Precision = precision,
                SqlDataType = sqlDataType,
                DataType = dataType,
                Value = value,
                IsOutput = isOutput
            };
            if (!string.IsNullOrEmpty(tableName) && p.TryPopulateField(tableName,fieldName))
            {
                p.Name = name;
            }
            proc.Parameters.Add(p);
            return proc;
        }

        public static SqlBuilder Builder(this StoredProcedure proc)
        {
            return proc.Builder.Builder();
        }


        public static StoredProcedure Parameter<T>(this StoredProcedure proc, string tableName, string name, T value, string fromField = null)
        {
            return Parameter(proc, name, SqlDbType.VarChar, value, typeof(T),tableName: tableName,fieldName: fromField);
        }

        public static StoredProcedure Parameter<T>(this StoredProcedure proc, string name, SqlDbType sqlDataType, T value, int maxLength = -1, int scale = -1, int precision = -1)
        {

            return Parameter(proc, name, sqlDataType, value, typeof(T), maxLength, scale, precision);
        }

        public static StoredProcedure Output<T>(this StoredProcedure proc, string name, SqlDbType sqlDataType, int maxLength = -1, int scale = -1, int precision = -1)
        {

            return Parameter(proc, name, sqlDataType, null, typeof(T), maxLength, scale, precision, true);
        }



        #endregion

        #region Update statement

        public static UpdateTable Table(this SqlBuilder builder, string tableName, string schema = null)
        {
            if (builder.Tables.Count > 0 && builder.BaseTable() is UpdateTable)
            {
                return builder.BaseTable() as UpdateTable;
            }
            else
            {
                UpdateTable t = new UpdateTable(builder, tableName, schema);
                builder.Tables.Add(t);
                return t;
            }
        }

        public static UpdateTable Set(this UpdateTable table, string fieldName, object value, SqlDbType sqlDataType, Type dataType, int maxLength = -1, int precision = -1, int scale = -1)
        {
            table.FieldList.Add(new ParameterField()
            {
                Builder = table.Builder,
                Name = fieldName,
                ParameterName = "@" + fieldName,
                MaxLength = maxLength,
                Precision = precision,
                Scale = scale,
                SqlDataType = sqlDataType,
                DataType = dataType,
                Value = value,
                Table = table

            }.PopulateField());
            return table;
        }

        public static UpdateTable Set<T>(this UpdateTable table, string fieldName, T value, SqlDbType dataType, int maxLength = -1, int scale = -1)
        {
            table.FieldList.Add(new ParameterField<T>()
            {
                Builder = table.Builder,
                Name = fieldName,
                ParameterName = "@" + fieldName,
                MaxLength = maxLength,
                Scale = scale,
                SqlDataType = dataType,
                FieldValue = value,
                Table = table,
                DataType = value.GetType()
            }.PopulateField());
            return table;
        }

        public static TableParameterField Output(this UpdateTable table, string parameterName = null)
        {
            if (parameterName == null)
            {
                parameterName = "output" + table.Name.Replace(".", "");
            }
            table.Output = new TableParameterField()
            {
                Name = parameterName,
                ParameterName = "@" + parameterName,
                ParameterTable = new Table(table.Builder, "inserted", ""),
                Builder = table.Builder,
                Table = table
            };
            table.Output.PopulateField();
            return table.Output;
        }

        public static TableParameterField PrimaryKey(this TableParameterField table)
        {
            MetadataTable mt = table.Table.Builder.Metadata.FindTable(table.Table.FullName);
            if (mt == null)
            {
                throw new InvalidOperationException("Metadata for the table " + table.Table.FullName + " could not be loaded");
            }
            foreach (MetadataColumn col in mt.PrimaryKey.Columns)
            {
                table.Column(col.Name, col.SqlDataType, col.Length, col.Precision, col.Scale);
            }
            return table;
        }
        public static TableParameterField Column(this TableParameterField table, string fieldName, SqlDbType dataType, int maxLength = -1, int precision = -1, int scale = -1)
        {

            table.ParameterTable.FieldList.Add(new Field()
            {
                Builder = table.Builder,
                Name = fieldName,
                MaxLength = maxLength,
                Precision = precision,
                Scale = scale,
                SqlDataType = dataType,
                Table = table.ParameterTable
            }.PopulateField()
            );
            return table;
        }

        public static UpdateTable UpdateTable(this TableParameterField table)
        {
            return table.Builder.BaseTable() as UpdateTable;
        }



        #endregion

        #region Insert Statement

        public static TableParameterField Output(this InsertIntoTable table, string parameterName = null)
        {
            if (parameterName == null)
            {
                parameterName = "output" + table.Name;
            }
            table.Output = new TableParameterField()
            {
                ParameterName = "@" + parameterName,
                Name = parameterName,
                ParameterTable = new Table(table.Builder, "inserted", ""),
                Builder = table.Builder,
                Table = table
            };
            return table.Output;
        }

        public static InsertIntoTable InsertTable(this TableParameterField table)
        {
            return table.Table as InsertIntoTable;
        }



        public static InsertIntoTable Into(this SqlBuilder builder, string tableName, string schema = null)
        {
            if (builder.Tables.Count > 0 && builder.BaseTable() is InsertIntoTable)
            {
                return builder.BaseTable() as InsertIntoTable;
            }
            else
            {
                InsertIntoTable t = new InsertIntoTable(builder, tableName);
                t.Schema = schema;
                builder.Tables.Add(t);
                return t;
            }
        }

        public static InsertIntoTable Value(this InsertIntoTable table, string fieldName, object value)
        {
            
            Field f = new ParameterField()
            {
                Builder = table.Builder,
                Table = table,
                DataType = value == null ? null : value.GetType(),
                Name = fieldName,
                ParameterName = "@" + fieldName,
                Value = value
            };
            table.FieldList.Add(f.PopulateField());
            return table;
        }

        public static InsertIntoTable Value<T>(this InsertIntoTable table, string fieldName, T value, SqlDbType dataType, int maxLength = -1, int scale = -1)
        {
            Field f = new ParameterField<T>()
            {
                Builder = table.Builder,
                Name = fieldName,
                ParameterName = "@" + fieldName,
                MaxLength = maxLength,
                Scale = scale,
                SqlDataType = dataType,
                FieldValue = value
            };

            table.FieldList.Add(f.PopulateField());
            return table;
        }



        #endregion

        #region Functions
        public static BuiltinFn Fn(this Table table)
        {
            return BuiltinFn.Fn(table.Builder, table);
        }
        public static Table ToTable(this BuiltinFn fn)
        {
            return fn.Table;
        }


        #endregion

        #region Table

        public static SqlBuilder Builder(this Table table)
        {
            SqlBuilder b = table.Builder;
            while (b.ParentBuilder != null)
            {
                b = b.ParentBuilder;
            }
            return b;
        }

        public static SqlBuilder Builder(this WhereConditionGroup group)
        {
            return group.Builder.Builder();
        }
        public static SqlBuilder Builder(this SqlBuilder builder)
        {
            SqlBuilder b = builder;
            while (b.ParentBuilder != null)
            {
                b = b.ParentBuilder;
            }
            return b;
        }

        public static SqlBuilder Builder(this TableParameterField field)
        {
            return field.Builder.Builder();
        }



        public static Table From(this Table sql, string tableName, string alias = null)
        {
            return sql.Builder.From(tableName, alias);
        }
        public static Table From(this SqlBuilder sql, string tableName, string alias = null, string schema = null)
        {
            Table table = sql.FindTable(alias ?? tableName, schema);
            if (table != null && !sql.JoinConditions.Select(x => x.ToTable).Any(x => x.Equals(table)))
            {
                return table;
            }
            table = new Table(sql, tableName, string.IsNullOrEmpty(alias) ? "t" + sql.Tables.Count.ToString() : alias, schema);
            sql.Tables.Add(table);
            return table;
        }
        public static Table ToTable(this JoinConditionGroup group)
        {
            return group.Join.ToTable;
        }

        public static Table Into(this Table table, string tempTable, bool outputTable = true)
        {
            if (table.Builder.SelectIntoTable != null)
            {
                return table;
            }
            table.Builder.SelectIntoTable = new TempTable()
            {
                Builder = table.Builder,
                Name = tempTable
            };
            table.Builder.SelectIntoTable.AllColumns(true);
            return table;
        }

        public static TempTable OrderBy(this TempTable table, string fieldName, OrderByDirections direction)
        {
            Field field = table.FindField(fieldName);
            if (field == null)
            {
                if (table.FieldList.Any(x => x.Name.Equals("*")))
                {
                    field = new Field()
                    {
                        Builder = table.Builder,
                        Table = table,
                        Alias = null,
                        Name = fieldName
                    };
                }
                else
                {
                    throw new InvalidOperationException(string.Format("The field '{0}' was not found in the table '{1}'", fieldName, table.Name));
                }
            }
            if (!table.OrderByClause.Any(x => x.Field.ReferenceName.Equals(field.ReferenceName)))
            {
                table.OrderByClause.Add(new OrderBy()
                {
                    Field = field,
                    Direction = direction
                });
            }
            return table;
        }

        public static Table OrderBy(this Table table, string fieldName, OrderByDirections direction)
        {
            Field field = table.FindField(fieldName);
            if (field == null)
            {
                if (table.FieldList.Any(x => x.Name.Equals("*")))
                {
                    field = new Field()
                    {
                        Builder = table.Builder,
                        Table = table,
                        Alias = null,
                        Name = fieldName
                    };
                }
                else
                {
                    throw new InvalidOperationException(string.Format("The field '{0}' was not found in the table '{1}'", fieldName, table.Name));
                }

            }
            table.Builder.OrderByClause.Add(new OrderBy()
                {
                    Field = field,
                    Direction = direction
                });
            return table;

        }

        #endregion

        #region SELECT list

        internal static void AllColumns(Table sql, MetadataTable mt)
        {
            foreach (MetadataColumn col in mt.Columns.Values)
            {
                Column(sql, col);
            }
        }

        internal static void Column(Table sql, MetadataColumn col, string alias = null)
        {
            if (sql.FindField(col.Name) == null)
            {
                Field f = new Field()
                {
                    Name = col.Name,
                    Alias = alias,
                    Table = sql,
                    Builder = sql.Builder
                };
                col.PopulateField<Field>(f);
                sql.FieldList.Add(f);
            }
        }


        public static Table AllColumns(this Table sql, bool useWildcardCharacter = false)
        {
            MetadataDatabase mdb = sql.Builder.Metadata;
            if (!useWildcardCharacter && mdb != null)
            {
                MetadataTable mt = mdb.FindTable(sql.FullName);
                if (mt == null)
                {
                    throw new ArgumentException(string.Format("The table {0} cannot be resolved with metadata. Must use wildcard", sql.FullName), "useWildcardCharacter");
                }
                AllColumns(sql, mt);
                return sql;
            }
            else
            {
                sql.FieldList.Add(new Field()
                {
                    Name = "*",
                    Alias = null,
                    Table = sql,
                    Builder = sql.Builder
                });
            }
            return sql;
        }

        public static Table ConcatColumns(this Table sql, string alias, string separator, params string[] columns)
        {
            string prefix = sql.Alias;
            string fieldName = columns[0];
            for (int i = 1; i < columns.Length; i++)
            {
                fieldName += string.Format(" + '{0}' + [{1}].{2}", separator, prefix, columns[i]);
            }
            sql.FieldList.Add(new Field()
            {
                Name = fieldName,
                Alias = alias,
                Table = sql,
                Builder = sql.Builder
            });
            return sql;
        }


        public static Table Columns(this Table sql, params string[] fields)
        {
            for (int i = 0; i < fields.Length; i++)
            {
                if (sql.FindField(fields[i]) == null)
                {
                    Column(sql, fields[i], null);
                }
            }
            return sql;
        }
        public static Table Column(this Table sql, string field, string alias = null)
        {
            Field f = new Field()
            {
                Name = field,
                Alias = alias,
                Table = sql,
                Builder = sql.Builder
            };
            MetadataDatabase mdb = sql.Builder.Metadata;
            if (mdb != null)
            {
                MetadataTable mt = mdb.FindTable(sql.FullName);
                if (mt != null)
                {
                    MetadataColumn mc = null;
                    if (mt.Columns.TryGetValue(field, out mc))
                    {
                        mc.PopulateField<Field>(f);
                    }
                }
            }
            sql.FieldList.Add(f);
            return sql;
        }

        public static Table Column<T>(this Table table, T value, string alias)
        {
            table.FieldList.Add(new ValueField<T>()
                {
                    Alias = alias,
                    FieldValue = value,
                    Table = table,
                    Builder = table.Builder
                });
            return table;
        }


        #endregion

        #region JOIN conditions

        public static Table SubSelect(this Table table, string tableName)
        {
            MetadataTable mt = table.Builder.Metadata.FindTable(table.FullName);
            if (mt == null)
            {
                throw new InvalidOperationException("Metadata for the table " + table.FullName + " could not be found");
            }
            MetadataTable mtTo = table.Builder.Metadata.FindTable(tableName);
            List<MetadataForeignKey> fks = mtTo.ForeignKeys.Values.Where(x => x.ReferencedTable == mt.Name && x.ReferencedSchema == mt.Schema).ToList();
            if (fks.Count != 1)
            {
                throw new InvalidOperationException(string.Format("Extended one relationship to the table {0}. Found {1}.", mtTo.Fullname, fks.Count));
            }

            MetadataForeignKey fk = fks.First();
            return table.SubSelect(mtTo.Name, mt.PrimaryKey.Columns.First().Name, fk.ColumnReferences.First().Name, null, mt.Schema, null);
        }

        public static Table SubSelect(this Table table, string tableName, string fromField, string toField, string alias = null, string schema = null, string builderName = null)
        {
            string key = table.Alias + "." + fromField + ":" + (alias ?? (schema != null ? schema + "." : "") + tableName) + "." + toField;
            SqlBuilder b;
            if (table.Builder.SubQueries.TryGetValue(key, out b))
            {
                return b.BaseTable();
            }
            string tmp = System.IO.Path.GetRandomFileName().Replace(".", "");
            TempTable into = table.Builder.SelectIntoTable;
            if (into == null)
            {
                table.Into(tmp, true);
                into = table.Builder.SelectIntoTable;
            }
            if (table.FindField(fromField) == null && !table.FieldList.Any(x => x.Name == "*"))
            {
                table.Column(fromField);
            }
            into.OrderBy(fromField, OrderByDirections.Asc);

            tmp = System.IO.Path.GetRandomFileName().Replace(".", "");
            b = SqlBuilder.Select()
                .From(tableName, alias, schema)
                .Column(toField)
                .Into(tmp, true)
                .WhereExists(table.Builder.SelectIntoTable)
                .And(into, toField, SqlOperators.Equal, fromField)
                .Builder.SelectIntoTable
                    .OrderBy(fromField, OrderByDirections.Asc)
                .Builder;

            b.BuilderName = builderName;
            table.Builder.AddSubQuery(key, b);
            return b.BaseTable();

        }


        public static Join InnerJoin(this Table sql, string tableName, string alias = null, string schema = null)
        {
            return MakeJoin(Join.JoinTypes.Inner, sql, tableName, alias, schema);
        }
        public static Join InnerJoin(this JoinConditionGroup group, string tableName, string alias = null, string schema = null)
        {
            return group.Join.FromTable.InnerJoin(tableName, alias, schema);
        }
        public static Join LeftOuterJoin(this Table sql, string tableName, string alias = null, string schema = null)
        {
            return MakeJoin(Join.JoinTypes.LeftOuter, sql, tableName, alias, schema);
        }
        public static Join LeftOuterJoin(this JoinConditionGroup group, string tableName, string alias = null, string schema = null)
        {
            return group.Join.FromTable.LeftOuterJoin(tableName, alias, schema);
        }
        public static Join RightOuterJoin(this Table sql, string tableName, string alias = null, string schema = null)
        {
            return MakeJoin(Join.JoinTypes.RightOuter, sql, tableName, alias, schema);
        }
        public static Join RightOuterJoin(this JoinConditionGroup group, string tableName, string alias = null, string schema = null)
        {
            return group.Join.FromTable.RightOuterJoin(tableName);
        }
        public static Join CrossJoin(this Table sql, string tableName)
        {
            return MakeJoin(Join.JoinTypes.Cross, sql, tableName);
        }
        public static Join CrossJoin(this JoinConditionGroup group, string tableName, string alias = null, string schema = null)
        {
            return group.Join.FromTable.CrossJoin(tableName);
        }

        internal static Join MakeJoin(Join.JoinTypes joinType, Table fromTable, string toTable, string alias = null, string schema = null)
        {
            Table right = fromTable.Builder.Tables.FirstOrDefault(x => x.Name.Equals((alias ?? toTable), StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(alias, StringComparison.InvariantCultureIgnoreCase)));
            if (fromTable.Builder.JoinConditions.Select(x => x.ToTable).Any(x => x.Equals(right)))
            {
                right = null;
            }


            if (right == null)
            {
                right = fromTable.Builder.From(toTable, alias, schema);
            }

            Join join = new Join()
            {
                JoinType = joinType,
                FromTable = fromTable,
                ToTable = right,
                Builder = fromTable.Builder
            };
            fromTable.Builder.JoinConditions.Add(join);
            return join;
        }

        public static ExistsConditionGroup And(this ExistsConditionGroup group, string fromField, SqlOperators Operator, string toField)
        {
            return ExistsConditionInternal(group, fromField, Operator, toField, group.Conditions.Count > 0 ? BoolOperators.And : BoolOperators.None);
        }

        public static ExistsConditionGroup And(this ExistsConditionGroup group, Table inTable, string inField, SqlOperators Operator, string toField)
        {
            return ExistsConditionInternal(group, inTable, inField, Operator, toField, group.Conditions.Count == 0 ? BoolOperators.None : BoolOperators.And);
        }

        public static ExistsConditionGroup And<T>(this ExistsConditionGroup group, string tableName, string fieldName, SqlOperators Operator, T value)
        {
            return WhereExistsInternal<T>(group, tableName, fieldName, Operator, BoolOperators.And, value);
        }
        public static ExistsConditionGroup Or<T>(this ExistsConditionGroup group, string tableName, string fieldName, SqlOperators Operator, T value)
        {
            return WhereExistsInternal<T>(group, tableName, fieldName, Operator, BoolOperators.Or, value);
        }

        private static ExistsConditionGroup WhereExistsInternal<T>(ExistsConditionGroup group, string tableName, string fieldName, SqlOperators Operator, BoolOperators linkType, T value)
        {
            Table t = null;
            if (group.InTable.Name.Equals(tableName, StringComparison.InvariantCultureIgnoreCase) || (group.InTable.Alias != null && group.InTable.Alias.Equals(tableName, StringComparison.InvariantCultureIgnoreCase)))
            {
                t = group.InTable;
            }
            if (t == null)
                t = group.Builder.Tables.FirstOrDefault(x => x.Name.Equals(tableName, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(tableName, StringComparison.InvariantCultureIgnoreCase)));
            if (t == null)
            {
                throw new InvalidOperationException(string.Format("The EXISTS table '{0}' does not exist", tableName));
            }

            ValueField<T> fv = new ValueField<T>()
            {
                Table = t,
                Name = fieldName,
                Builder = group.Builder,
                FieldValue = value

            };
            FieldCondition fc = new FieldCondition()
            {
                Builder = group.Builder,
                ConditionLink = group.Conditions.Count > 0 ? linkType : BoolOperators.None,
                ParentGroup = group,
                Condition = Operator,
                LeftTable = t,
                LeftField = fv
            };
            group.Conditions.Add(fc);
            if (group.SubConditions.Count > 0)
            {
                group.SubConditions.First().ConditionLink = (group.SubConditions.First().ConditionLink == BoolOperators.None ? group.SubConditions.First().ConditionLink = BoolOperators.And : group.SubConditions.First().ConditionLink);
            }
            return group;
        }

        public static ExistsConditionGroup Or(this ExistsConditionGroup group, string fromField, SqlOperators Operator, string toField)
        {
            return ExistsConditionInternal(group, fromField, Operator, toField, group.Conditions.Count > 0 ? BoolOperators.Or : BoolOperators.None);
        }

        public static ExistsConditionGroup AndGroup(this ExistsConditionGroup group)
        {
            return ExistsGroupInternal(group, BoolOperators.And);
        }

        private static ExistsConditionGroup ExistsGroupInternal(ExistsConditionGroup group, BoolOperators conditionLink)
        {
            ExistsConditionGroup g = new ExistsConditionGroup()
            {
                ConditionLink = conditionLink,
                Parent = group.Parent,
                Builder = group.Builder,
                FromTable = group.FromTable,
                InTable = group.InTable,
                Negated = group.Negated
            };
            group.SubConditions.Add(g);
            return g;
        }
        public static ExistsConditionGroup OrGroup(this ExistsConditionGroup group)
        {
            return ExistsGroupInternal(group, BoolOperators.Or);
        }

        private static ExistsConditionGroup ExistsConditionInternal(ExistsConditionGroup group, Table inTable, string inField, SqlOperators Operator, string toField, BoolOperators linkType)
        {
            // Field lf = FromTable.FieldList.FirstOrDefault(x => x.Name.Equals(FromField, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(FromField, StringComparison.InvariantCultureIgnoreCase)));
            Field lf = inTable.FindField(inField);
            if (lf == null || group.FromTable == null)
            {
                lf = new Field()
                {
                    Table = inTable,
                    Builder = group.Builder,
                    Name = inField,
                    Alias = null
                };
            }
            Table t = group.Builder.FindTable(group.FromTable);
            Field rf = null;
            if (t != null)
            {
                rf = t.FindField(toField);
            }
            // Field rf = group.InTable.FieldList.FirstOrDefault(x => x.Name.Equals(ToField, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(ToField, StringComparison.InvariantCultureIgnoreCase)));
            // Field rf = group.InTable.FieldList.FirstOrDefault(x => x.Name.Equals(ToField, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(ToField, StringComparison.InvariantCultureIgnoreCase)));
            if (rf == null)
            {
                rf = new Field()
                {
                    Table = t,
                    Builder = group.Builder,
                    Name = toField,
                    Alias = null
                };
            }

            FieldCondition condition = new FieldCondition()
            {
                Builder = group.Builder,
                LeftTable = inTable,
                LeftField = lf,
                RightTable = t,
                RightField = rf,
                Condition = Operator,
                ParentGroup = group,
                ConditionLink = linkType
            };
            group.Conditions.Add(condition);
            return group;
        }

        private static ExistsConditionGroup ExistsConditionInternal(ExistsConditionGroup group, string fromField, SqlOperators Operator, string toField, BoolOperators linkType)
        {
            Table fromTable = null;
            if (group.FromTable == null)
            {
                fromTable = group.Builder.Tables.First();
            }
            else
            {
                fromTable = group.Builder.Tables.FirstOrDefault(x => x.Name.Equals(group.FromTable) || (x.Alias != null && x.Alias.Equals(group.FromTable)));
            }
            return ExistsConditionInternal(group, group.InTable, fromField, Operator, toField, linkType);


        }



        public static JoinConditionGroup And(this JoinConditionGroup group, string fromField, SqlOperators Operator, string toField)
        {
            return OnInternal(group, fromField, Operator, toField, BoolOperators.And);
        }
        public static JoinConditionGroup Or(this JoinConditionGroup group, string fromField, SqlOperators Operator, string toField)
        {
            return OnInternal(group, fromField, Operator, toField, BoolOperators.Or);
        }

        public static JoinConditionGroup AndGroup(this JoinConditionGroup group)
        {
            JoinConditionGroup g = new JoinConditionGroup()
            {
                ConditionLink = BoolOperators.And,
                Parent = group.Parent,
                Join = group.Join
            };
            group.SubConditions.Add(g);
            return g;
        }

        public static JoinConditionGroup OrGroup(this JoinConditionGroup group)
        {
            JoinConditionGroup g = new JoinConditionGroup()
            {
                ConditionLink = BoolOperators.Or,
                Parent = group.Parent,
                Join = group.Join
            };
            group.SubConditions.Add(g);
            return g;
        }

        public static JoinConditionGroup On(this Join join, string fromField, SqlOperators Operator, string toField)
        {
            return OnInternal(join.Conditions, fromField, Operator, toField, BoolOperators.None);
        }

        public static WhereConditionGroup Where<T>(this Table table, string tableName, string fieldName, SqlOperators Operator, T value)
        {
            return (WhereConditionGroup)WhereInternal<T>(table.Builder.WhereConditions, tableName, fieldName, Operator, value);
        }

        public static ExistsConditionGroup WhereExists(this Table table, string inTable)
        {
            return ExistsInternal(table.Builder.WhereConditions, inTable, table.Alias, table.Builder.WhereConditions.Conditions.Count > 0 ? BoolOperators.And : BoolOperators.None, false);
        }

        public static ExistsConditionGroup WhereExists(this Table table, Table inTable)
        {
            return ExistsInternal(table.Builder.WhereConditions, inTable.Alias, table.Alias, table.Builder.WhereConditions.Conditions.Count > 0 ? BoolOperators.And : BoolOperators.None, false);
        }

        public static ExistsConditionGroup WhereNotExists(this Table table, string inTable)
        {
            return ExistsInternal(table.Builder.WhereConditions, inTable, table.Alias, table.Builder.WhereConditions.Conditions.Count > 0 ? BoolOperators.And : BoolOperators.None, true);
        }

        public static ExistsConditionGroup AndExists(this WhereConditionGroup group, string inTable, string fromTable = null)
        {
            return ExistsInternal(group, inTable, fromTable, group.Conditions.Count > 0 ? BoolOperators.And : BoolOperators.None, false);
        }
        public static ExistsConditionGroup AndNotExists(this WhereConditionGroup group, string inTable, string fromtable = null)
        {
            return ExistsInternal(group, inTable, fromtable, group.Conditions.Count > 0 ? BoolOperators.And : BoolOperators.None, true);
        }

        public static WhereConditionGroup EndExists(this ExistsConditionGroup group)
        {
            return (WhereConditionGroup)group.Parent;
        }

        private static ExistsConditionGroup ExistsInternal(WhereConditionGroup group, string inTable, string fromTable, BoolOperators conditionLink, bool negated)
        {
            Table t = new Table()
            {
                Name = inTable,
                Builder = group.Builder,
                Alias = null
            };
            ExistsConditionGroup exists = new ExistsConditionGroup()
            {
                Builder = group.Builder,
                ConditionLink = conditionLink,
                Negated = negated,
                Parent = group,
                InTable = t,
                FromTable = fromTable
            };
            group.SubConditions.Add(exists);

            return exists;
        }

        public static JoinConditionGroup And(this JoinConditionGroup group, string fieldName, SqlOperators Operator, object value, string tableName = null)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                tableName = group.Join.ToTable.Alias;
            }
            return (JoinConditionGroup)WhereInternalAll(@group, tableName, fieldName, Operator, value, BoolOperators.And);
        }

        public static JoinConditionGroup And<T>(this JoinConditionGroup group, string fieldName, SqlOperators Operator, T value, string tableName = null)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                tableName = group.Join.ToTable.Alias;
            }
            return (JoinConditionGroup)WhereInternalAll<T>(@group, tableName, fieldName, Operator, value, BoolOperators.And);
        }
        public static JoinConditionGroup Or<T>(this JoinConditionGroup group, string fieldName, SqlOperators Operator, T value, string tableName = null)
        {
            if (string.IsNullOrEmpty(tableName))
            {
                tableName = group.Join.ToTable.Alias;
            }
            return (JoinConditionGroup)WhereInternalAll<T>(@group, tableName, fieldName, Operator, value, BoolOperators.Or);
        }


        public static WhereConditionGroup And<T>(this ConditionGroup group, string tableName, string fieldName, SqlOperators Operator, T value)
        {
            return (WhereConditionGroup)WhereInternal<T>(group, tableName, fieldName, Operator, value, BoolOperators.And);
        }

        public static WhereConditionGroup Or<T>(this ConditionGroup group, string tableName, string fieldName, SqlOperators Operator, T value)
        {
            return (WhereConditionGroup)WhereInternal<T>(group, tableName, fieldName, Operator, value, BoolOperators.Or);
        }

        public static WhereConditionGroup AndGroup(this WhereConditionGroup group)
        {
            WhereConditionGroup g = new WhereConditionGroup()
            {
                ConditionLink = BoolOperators.And,
                Parent = group,
                Builder = group.Builder
            };
            group.SubConditions.Add(g);
            return g;
        }

        public static WhereConditionGroup OrGroup(this WhereConditionGroup group)
        {
            WhereConditionGroup g = new WhereConditionGroup()
            {
                ConditionLink = BoolOperators.Or,
                Parent = group,
                Builder = group.Builder
            };
            group.SubConditions.Add(g);
            return g;
        }

        private static ConditionGroup WhereInternalAll<T>(ConditionGroup group, string tableName, string fieldName, SqlOperators Operator, T value, BoolOperators linkType = BoolOperators.None)
        {

            Table t = group.Builder.Tables.FirstOrDefault(x => x.Name.Equals(tableName, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(tableName, StringComparison.InvariantCultureIgnoreCase)));
            if (t == null)
            {
                throw new InvalidOperationException(string.Format("The WHERE condition table '{0}' does not exist", tableName));
            }
            ValueField<T> fv = new ValueField<T>()
            {
                Table = t,

                Name = fieldName,
                Builder = group.Builder,
                FieldValue = value

            };
            FieldCondition fc = new FieldCondition()
            {
                Builder = group.Builder,
                ConditionLink = group.Conditions.Count > 0 ? linkType : BoolOperators.None,
                ParentGroup = group,
                Condition = Operator,
                LeftTable = t,
                LeftField = fv
            };
            group.Conditions.Add(fc);
            if (group.SubConditions.Count > 0)
            {
                group.SubConditions.First().ConditionLink = (group.SubConditions.First().ConditionLink == BoolOperators.None ? group.SubConditions.First().ConditionLink = BoolOperators.And : group.SubConditions.First().ConditionLink);
            }
            return group;
        }

        internal static ConditionGroup WhereInternalAll(ConditionGroup group, string tableName, string fieldName, SqlOperators Operator, object value, BoolOperators linkType = BoolOperators.None)
        {

            Table t = group.Builder.Tables.FirstOrDefault(x => x.Name.Equals(tableName, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(tableName, StringComparison.InvariantCultureIgnoreCase)));
            if (t == null)
            {
                throw new InvalidOperationException(string.Format("The WHERE condition table '{0}' does not exist", tableName));
            }
            ValueField fv = new ValueField()
            {
                Table = t,
                Name = fieldName,
                Builder = group.Builder,
                Value = value,
                DataType = value.GetType()
            };
            fv.PopulateField();
            FieldCondition fc = new FieldCondition()
            {
                Builder = group.Builder,
                ConditionLink = group.Conditions.Count > 0 ? linkType : BoolOperators.None,
                ParentGroup = group,
                Condition = Operator,
                LeftTable = t,
                LeftField = fv
            };
            group.Conditions.Add(fc);
            if (group.SubConditions.Count > 0)
            {
                group.SubConditions.First().ConditionLink = (group.SubConditions.First().ConditionLink == BoolOperators.None ? group.SubConditions.First().ConditionLink = BoolOperators.And : group.SubConditions.First().ConditionLink);
            }
            return group;
        }



        private static ConditionGroup WhereInternal<T>(ConditionGroup group, string tableName, string fieldName, SqlOperators Operator, T value, BoolOperators linkType = BoolOperators.None)
        {

            Table t = group.Builder.Tables.FirstOrDefault(x => x.Name.Equals(tableName, StringComparison.InvariantCultureIgnoreCase) || (x.Alias != null && x.Alias.Equals(tableName, StringComparison.InvariantCultureIgnoreCase)) || x.FullName.Equals(tableName, StringComparison.InvariantCultureIgnoreCase));
            if (t == null)
            {
                throw new InvalidOperationException(string.Format("The WHERE condition table '{0}' does not exist", tableName));
            }
            Field f = t.FindField(fieldName);
            ValueField<T> fv = new ValueField<T>()
            {
                Table = t,
                Name = f != null ? f.Name : fieldName,
                Builder = group.Builder,
                FieldValue = value,
                DataType = value.GetType()
            };
            fv.TryPopulateField();
            FieldCondition fc = new FieldCondition()
            {
                Builder = group.Builder,
                ConditionLink = group.Conditions.Count > 0 ? linkType : BoolOperators.None,
                ParentGroup = group,
                Condition = Operator,
                LeftTable = t,
                LeftField = fv.PopulateField()
            };
            group.Conditions.Add(fc);
            if (group.SubConditions.Count > 0)
            {
                group.SubConditions.First().ConditionLink = (group.SubConditions.First().ConditionLink == BoolOperators.None ? group.SubConditions.First().ConditionLink = BoolOperators.And : group.SubConditions.First().ConditionLink);
            }
            return group;
        }

        internal static JoinConditionGroup OnInternal(JoinConditionGroup group, Field fromField, SqlOperators Operator, Field toField, BoolOperators linkType = BoolOperators.None)
        {
            Join join = group.Join;
            FieldCondition condition = new FieldCondition()
            {
                Builder = join.Builder,
                LeftTable = join.FromTable,
                LeftField = fromField,
                RightTable = join.ToTable,
                RightField = toField,
                Condition = Operator,
                ParentGroup = group,
                ConditionLink = linkType
            };
            group.Conditions.Add(condition);
            return group;
        }

        internal static Field PopulateField(this Field f)
        {
            f.TryPopulateField();
            return f;
        }

        //internal static ParameterField PopulateField(this ParameterField f)
        //{
        //    f.TryPopulateField();
        //    return f;
        //}

        //internal static bool TryPopulateField(this ParameterField f, string UseTable = null, string UseField = null)
        //{
        //    if (f.Table == null && string.IsNullOrEmpty(UseTable))
        //    {
        //        return false;
        //    }

        //    MetadataDatabase mdb = f.Table != null ? f.Table.Builder.Metadata : f.Builder.Metadata;

        //    if (mdb == null)
        //    {
        //        return false;
        //    }
        //    MetadataTable mt = mdb.FindTable(string.IsNullOrEmpty(UseTable) ? f.Table.FullName : UseTable);
        //    if (mt == null)
        //    {
        //        return false;
        //    }
        //    MetadataColumn mc = null;
        //    if (!mt.Columns.TryGetValue(string.IsNullOrEmpty(UseField) ? f.Name : UseField, out mc))
        //    {
        //        return false;
        //    }
        //    mc.PopulateField<Field>(f);
        //    return true;
        //}


        internal static bool TryPopulateField(this Field f, string useTable = null, string useField = null)
        {
            if (f.Table == null && string.IsNullOrEmpty(useTable))
            {
                return false;
            }

            MetadataDatabase mdb = f.Table != null ? f.Table.Builder.Metadata : f.Builder.Metadata;
            
            if (mdb == null)
            {
                return false;
            }
            MetadataTable mt = mdb.FindTable(string.IsNullOrEmpty(useTable) ? f.Table.FullName : useTable);
            if (mt == null)
            {
                return false;
            }
            MetadataColumn mc = null;
            if (!mt.Columns.TryGetValue(string.IsNullOrEmpty(useField) ? f.Name : useField, out mc))
            {
                return false;
            }
            mc.PopulateField<Field>(f);
            return true;
        }

        internal static JoinConditionGroup OnInternal(JoinConditionGroup group, string fromField, SqlOperators Operator, string toField, BoolOperators linkType = BoolOperators.None)
        {
            Join join = group.Join;
            Field lf = join.FromTable.FindField(fromField);
            if (lf == null)
            {
                lf = new Field()
                {
                    Table = join.FromTable,
                    Builder = join.Builder,
                    Name = fromField,
                    Alias = null
                };
                lf.TryPopulateField();
            }
            Field rf = join.ToTable.FindField(toField);
            if (rf == null)
            {
                rf = new Field()
                {
                    Table = join.ToTable,
                    Builder = join.Builder,
                    Name = toField,
                    Alias = null
                };
                rf.TryPopulateField();
            }
            return OnInternal(group, lf, Operator, rf, linkType);
        }

        #endregion

        #region If Statements

        public static SqlBuilder Begin(this ConditionGroup group, SqlBuilder.StatementTypes statementType)
        {
            if (group.Builder is IfStatement)
            {
                return Begin((IfStatement)group.Builder, statementType);
            }
            else
            {
                throw new ArgumentException("The Begin() Extension can only be used for If statements", "End");
            }
        }

        public static SqlBuilder Begin(this IfStatement builder, SqlBuilder.StatementTypes statementType)
        {
            builder.StatementBody.StatementType = statementType;
            return builder.StatementBody;
        }
        public static IfStatement End(this SqlBuilder builder)
        {
            SqlBuilder sb = builder.ParentBuilder;
            while (sb != null && (!(sb is IfStatement) || (sb as IfStatement).BranchStatement != BranchStatements.If))
            {
                sb = sb.ParentBuilder;
            }
            if (sb != null)
            {
                return (IfStatement)sb;
            }
            else
            {
                throw new ArgumentException("The End() Extension can only be used for If statements", "End");
            }
        }

        public static IfStatement Else(this IfStatement builder)
        {
            return BranchInternal(builder, BranchStatements.Else);
        }

        public static IfStatement ElseIf(this IfStatement builder)
        {
            return BranchInternal(builder, BranchStatements.Else);
        }

        private static IfStatement BranchInternal(IfStatement builder, BranchStatements branch)
        {
            IfStatement statement = new IfStatement()
            {
                BranchStatement = branch,
                ParentBuilder = builder
            };
            builder.ElseIfStatements.Add(statement);
            return statement;
        }




        #endregion

    }
}
