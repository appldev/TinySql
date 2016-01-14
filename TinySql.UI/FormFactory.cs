using System;
using System.Collections.Generic;
using System.Collections.Concurrent;
using System.Linq;
using TinySql.Metadata;

namespace TinySql.UI
{
    public class FormFactory
    {
        private ConcurrentDictionary<string, Form> _PrimaryForms = new ConcurrentDictionary<string, Form>();

        public ConcurrentDictionary<string, Form> PrimaryForms
        {
            get { return _PrimaryForms; }
        }

        private static FormFactory instance = null;
        public static FormFactory Default
        {
            get
            {
                if (instance == null)
                {
                    instance = new FormFactory();
                }
                return instance;
            }
        }

        public Form GetForm(MetadataTable Table, FormTypes FormType, FormLayouts FormLayout = FormLayouts.Vertical, SectionLayouts SectionLayout = SectionLayouts.VerticalTwoColumns)
        {
            if (FormType == FormTypes.Mobile)
            {
                throw new NotSupportedException("Mobile forms are not supported");
            }
            Form f;
            if (PrimaryForms.TryGetValue(Table.Fullname, out f))
            {
                return f;
            }
            else
            {
                f = BuildForm(Table, FormType, FormLayout, SectionLayout);
                if (PrimaryForms.TryAdd(Table.Fullname, f))
                {
                    return f;
                }
                else
                {
                    throw new InvalidOperationException("The default form for " + Table.Fullname + " could not be cached");
                }
            }
        }

        public Form BuildForm(SqlBuilder Builder, FormLayouts FormLayout = FormLayouts.Vertical, SectionLayouts SectionLayout = SectionLayouts.VerticalTwoColumns)
        {
            Form form = new Form();
            form.FormLayout = FormLayout;
            for (int i = 0; i < Builder.Tables.Count; i++)
            {
                FormSection section = new FormSection() { SectionLayout = SectionLayout };
                Table table = Builder.Tables[i];
                MetadataTable mt = Builder.Metadata.FindTable(table.FullName);
                // section.Legend = StringMap.Default.GetText(SqlBuilder.DefaultCulture.LCID, mt.Fullname, mt.Name);
                section.Legend = mt.DisplayName;
                string TableName = mt.Fullname;
                if (i == 0)
                {
                    form.TitleColumn = mt.GuessTitleColumn();
                }
                foreach (Field field in table.FieldList)
                {
                    string name = field.Name;
                    MetadataColumn mc;
                    if (mt.Columns.TryGetValue(name, out mc))
                    {
                        MetadataColumn includedFrom = mt.Columns.Values.FirstOrDefault(x => x.IncludeColumns != null && x.IncludeColumns.Contains(field.Alias ?? field.Name));
                        BuildField(mc, TableName, field.Alias, section, includedFrom, i > 0);
                    }
                }
                if (section.Fields.Count > 0)
                {
                    form.Sections.Add(section);
                }
            }
            form.Sections[0].Fields.Insert(0, new FormField() { Name = "__TABLE", ID = "__TABLE", FieldType = FieldTypes.Input, InputType = InputTypes.text, IsHidden = true });
            return form;
        }

        public Form BuildForm(RowData Data, FormLayouts FormLayout = FormLayouts.Vertical, SectionLayouts SectionLayout = SectionLayouts.Vertical, MetadataTable Table = null)
        {
            if (Table == null)
            {
                Table = Data.Metadata;
                if (Table == null)
                {
                    throw new ArgumentException("The Metadata Table argument is null, and metadata cannot be retrieved from the RowData object", "Table");
                }
            }
            Form form = new Form();
            form.FormLayout = FormLayout;
            form.TitleColumn = Table.GuessTitleColumn();

            FormSection section = new FormSection();
            section.SectionLayout = SectionLayout;
            List<string> Columns = Data.Columns;
            string TableName = Table.Fullname;
            foreach (string s in Columns)
            {
                MetadataColumn mc;
                if (Table.Columns.TryGetValue(s, out mc))
                {
                    BuildField(mc, TableName, null, section, null, false);
                }
            }
            form.Sections.Add(section);
            form.Initialize(Data, Table);
            return form;
        }

        public Form BuildForm(MetadataTable Table, FormTypes FormType, FormLayouts FormLayout = FormLayouts.Vertical, SectionLayouts SectionLayout = SectionLayouts.VerticalTwoColumns)
        {
            Form form = new Form();
            form.FormLayout = FormLayout;
            form.TitleColumn = Table.GuessTitleColumn();
            if (form.TitleColumn == null)
            {
                form.TitleColumn = Table.PrimaryKey.Columns.First().Name;
            }
            FormSection section = new FormSection() { SectionLayout = SectionLayout };
            string TableName = Table.Fullname;
            foreach (MetadataColumn col in Table.Columns.Values)
            {
                BuildField(col, TableName, null, section, null, false);
            }
            form.Sections.Add(section);
            return form;
        }

        

        public static void BuildField(MetadataColumn Column, string Alias, FormSection Section, bool ForceReadonly = false)
        {
            BuildField(Column, Column.Parent.Schema + "." + Column.Parent.Name, Alias, Section, null, ForceReadonly);
        }

        private static void BuildField(MetadataColumn col, string TableName, string Alias, FormSection section, MetadataColumn IncludeFrom, bool ForceReadOnly = false)
        {
            if (col.IsRowGuid)
            {
                return;
            }
            FormField field = null;

            if (col.IsForeignKey)
            {
                field = new LookupFormField()
                {
                    LookupSource = LookupSources.SqlBuilder,
                    Builder = col.ToSqlBuilder()
                };
            }
            else
            {
                field = new FormField();
            }


            field.DisplayName = (IncludeFrom != null ? IncludeFrom.DisplayName + " " : "") + col.DisplayName;
            field.ID = col.Name;
            field.Name = col.Name;
            field.Alias = Alias;
            field.TableName = TableName;
            field.NullText = "Enter " + col.Name;
            field.IsReadOnly = ForceReadOnly || col.IsReadOnly || col.IsPrimaryKey;
            field.IsRequired = !col.Nullable;
            ResolveFieldType(col, field);
            section.Fields.Add(field);
        }

        private static void ResolveFieldType(MetadataColumn col, FormField field)
        {
            switch (col.SqlDataType)
            {
                case System.Data.SqlDbType.Text:
                case System.Data.SqlDbType.NText:
                case System.Data.SqlDbType.VarChar:
                case System.Data.SqlDbType.NVarChar:
                case System.Data.SqlDbType.Char:
                case System.Data.SqlDbType.NChar:
                    field.FieldType = FieldTypes.Input;
                    field.InputType = InputTypes.text;
                    if (col.Length <= 0)
                    {
                        field.FieldType = FieldTypes.TextArea;
                    }
                    field.MaxLength = col.Length;
                    break;

                case System.Data.SqlDbType.Date:
                    field.FieldType = FieldTypes.Input;
                    field.InputType = InputTypes.date;
                    break;

                case System.Data.SqlDbType.SmallDateTime:
                case System.Data.SqlDbType.DateTime:
                case System.Data.SqlDbType.DateTime2:
                    field.FieldType = FieldTypes.Input;
                    field.InputType = InputTypes.datetime_local;
                    break;

                case System.Data.SqlDbType.DateTimeOffset:
                    field.FieldType = FieldTypes.Input;
                    field.InputType = InputTypes.datetime;
                    break;

                case System.Data.SqlDbType.Time:
                    field.FieldType = FieldTypes.Input;
                    field.InputType = InputTypes.time;
                    break;

                case System.Data.SqlDbType.Bit:
                    field.FieldType = FieldTypes.Checkbox;
                    break;

                case System.Data.SqlDbType.Decimal:
                case System.Data.SqlDbType.Float:
                case System.Data.SqlDbType.Int:
                case System.Data.SqlDbType.Money:
                case System.Data.SqlDbType.BigInt:
                case System.Data.SqlDbType.Real:
                case System.Data.SqlDbType.SmallInt:
                case System.Data.SqlDbType.SmallMoney:
                case System.Data.SqlDbType.TinyInt:
                    field.FieldType = FieldTypes.Input;
                    field.InputType = InputTypes.number;
                    break;

                case System.Data.SqlDbType.VarBinary:
                case System.Data.SqlDbType.Timestamp:
                case System.Data.SqlDbType.Structured:
                case System.Data.SqlDbType.Binary:
                case System.Data.SqlDbType.Variant:
                case System.Data.SqlDbType.Xml:
                case System.Data.SqlDbType.Image:
                case System.Data.SqlDbType.Udt:
                case System.Data.SqlDbType.UniqueIdentifier:
                default:
                    field.FieldType = FieldTypes.Input;
                    field.IsHidden = true;
                    return;
            }

            // Special cases
            if (col.IsForeignKey)
            {
                field.FieldType = FieldTypes.SelectList;
            }

        }



    }
}
