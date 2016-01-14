using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Linq;
using TinySql.Metadata;

namespace TinySql.UI
{
    public enum FormTypes
    {
        Primary = 1,
        Mobile = 2,
    }

    public enum FormLayouts
    {
        Vertical = 1,
        Horizontal = 2,
        Inline = 3
    }

    public enum SectionLayouts
    {
        Vertical = 1,
        VerticalTwoColumns = 2,
        HorizontalOneColumn = 3,
        HorizontalTwoColumns = 4,
    }

    public enum FieldTypes
    {
        Input = 1,
        Checkbox = 2,
        Option = 3,
        SelectList = 4,
        LookupInput = 5,
        TextArea = 6
    }

    public enum LookupSources
    {
        NameValueCollection = 1,
        SqlBuilder = 2
    }

    public enum InputTypes
    {
        text,
        password,
        datetime,
        datetime_local,
        date,
        month,
        time,
        week,
        number,
        email,
        url,
        search,
        tel,
        color,
        file
    }

    public class FormDefaults
    {
        private static FormDefaults _instance = null;
        public static FormDefaults Default
        {
            get
            {
                if (_instance == null)
                {
                    _instance = new FormDefaults();
                }
                return _instance;
            }
        }

        private FormLayouts _FormLayout = FormLayouts.Vertical;

        public FormLayouts FormLayout
        {
            get { return _FormLayout; }
            set { _FormLayout = value; }
        }

        private string _CssFormLayout = null;
        public string CssFormLayout
        {
            get
            {
                if (_CssFormLayout != null)
                {
                    return _CssFormLayout;
                }
                switch (FormLayout)
                {

                    case FormLayouts.Horizontal:
                        return "form-horizontal";
                    case FormLayouts.Inline:
                        return "form-inline";
                    case FormLayouts.Vertical:
                    default:
                        return "";
                }
            }
        }


        private string _CssLabelLayoutVertical = "";
        public string CssLabelLayoutVertical
        {
            get { return _CssLabelLayoutVertical; }
            set { _CssLabelLayoutVertical = value; }
        }
        private string _CssLabelLayoutHorizontalOneColumn = "col-sm-2 control-label";

        public string CssLabelLayoutHorizontalOneColumn
        {
            get { return _CssLabelLayoutHorizontalOneColumn; }
            set { _CssLabelLayoutHorizontalOneColumn = value; }
        }
        private string _CssLabelLayoutHorizontalTwoColumns = "col-sm-2 control-label";

        public string CssLabelLayoutHorizontalTwoColumns
        {
            get { return _CssLabelLayoutHorizontalTwoColumns; }
            set { _CssLabelLayoutHorizontalTwoColumns = value; }
        }

        private string _CssInputLayoutVertical = "";
        public string CssInputLayoutVertical
        {
            get { return _CssInputLayoutVertical; }
            set { _CssInputLayoutVertical = value; }
        }
        private string _CssInputLayoutHorizontalOneColumn = "col-sm-10";

        public string CssInputLayoutHorizontalOneColumn
        {
            get { return _CssInputLayoutHorizontalOneColumn; }
            set { _CssInputLayoutHorizontalOneColumn = value; }
        }
        private string _CssInputLayoutHorizontalTwoColumns = "col-sm-10";

        public string CssInputLayoutHorizontalTwoColumns
        {
            get { return _CssInputLayoutHorizontalTwoColumns; }
            set { _CssInputLayoutHorizontalTwoColumns = value; }
        }

        private string _CssInputControlLayout = "form-control";

        public string CssInputControlLayout
        {
            get { return _CssInputControlLayout; }
            set { _CssInputControlLayout = value; }
        }

        private string _CssCheckBoxLayout = "checkbox";

        public string CssCheckBoxLayout
        {
            get { return _CssCheckBoxLayout; }
            set { _CssCheckBoxLayout = value; }
        }

        private string _CssFormSection = "row";

        public string CssFormSection
        {
            get { return _CssFormSection; }
            set { _CssFormSection = value; }
        }

        private int _MultiLineInputRows = 3;

        public int MultiLineInputRows
        {
            get { return _MultiLineInputRows; }
            set { _MultiLineInputRows = value; }
        }

        private SectionLayouts _SectionLayout = SectionLayouts.VerticalTwoColumns;

        public SectionLayouts SectionLayout
        {
            get { return _SectionLayout; }
            set { _SectionLayout = value; }
        }

        private string _CssSectionTwoColumns = "col-md-6";

        public string CssSectionTwoColumns
        {
            get { return _CssSectionTwoColumns; }
            set { _CssSectionTwoColumns = value; }
        }


        private string _CssFieldGroup = "form-group";

        public string CssFieldGroup
        {
            get { return _CssFieldGroup; }
            set { _CssFieldGroup = value; }
        }


        private string _CssLookupButtonClass = "glyphicon glyphicon-option-horizontal";
        public string CssLookupButtonClass
        {
            get { return _CssLookupButtonClass; }
            set { _CssLookupButtonClass = value; }
        }

        private string _CssLookupButtonClearClass = "glyphicon glyphicon-remove";

        public string CssLookupButtonClearClass
        {
            get { return _CssLookupButtonClearClass; }
            set { _CssLookupButtonClearClass = value; }
        }



        private string _CssSectionView = "~/Views/TinySql/Details/_Section.cshtml";

        public string CssSectionView
        {
            get { return _CssSectionView; }
            set { _CssSectionView = value; }
        }

        private string _EditDialogViewUrl = "~/Views/TinySql/Details/dialog.cshtml";

        public string EditDialogViewUrl
        {
            get { return _EditDialogViewUrl; }
            set { _EditDialogViewUrl = value; }
        }



    }

    public class Form
    {
        private string _ID = DateTime.Now.Ticks.ToString();
        public string ID
        {
            get { return _ID; }
            set { _ID = value; }
        }


        public Form()
        {

        }
        public string TitleColumn { get; set; }
        public string TitleTemplate { get; set; }

        public string Title
        {
            get
            {
                return Convert.ToString(Data.Column(TitleColumn));
            }
        }

        private RowData data = null;

        public RowData Data
        {
            get { return data; }
        }

        public void Initialize(RowData data, MetadataTable table = null)
        {
            this.data = data;
            this._Metadata = table;
        }

        private MetadataTable _Metadata = null;
        public MetadataTable Metadata
        {
            get
            {
                if (_Metadata == null)
                {
                    _Metadata = Data.Metadata;
                    if (_Metadata == null)
                    {
                        _Metadata = SqlBuilder.DefaultMetadata.FindTable(Data.Table);
                    }
                }
                return _Metadata;
            }
        }

        public SectionModel GetSectionModel(FormSection Section)
        {
            return new SectionModel(Section, this.Metadata, this.Data);
        }

        private List<FormSection> _Sections = new List<FormSection>();

        public List<FormSection> Sections
        {
            get { return _Sections; }
            set { _Sections = value; }
        }

        private FormLayouts? _FormLayout = null;

        public FormLayouts? FormLayout
        {
            get { return _FormLayout ?? FormDefaults.Default.FormLayout; }
            set { _FormLayout = value; }
        }

        private string _CssFormLayout = null;
        public string CssFormLayout
        {
            get
            {
                return _CssFormLayout ?? FormDefaults.Default.CssFormLayout;
            }
            set
            {
                _CssFormLayout = value;
            }
        }

        private string _CssSectionView = null;

        public string CssSectionView
        {
            get { return _CssSectionView ?? FormDefaults.Default.CssSectionView; }
            set { _CssSectionView = value; }
        }

        public FormField GetFormFieldByID(string ID)
        {
            return this.Sections.SelectMany(x => x.Fields).FirstOrDefault(x => x.ID.Equals(ID, StringComparison.OrdinalIgnoreCase));
        }

        private string _EditDialogViewUrl = null;

        public string EditDialogViewUrl
        {
            get { return _EditDialogViewUrl ?? FormDefaults.Default.EditDialogViewUrl; }
            set { _EditDialogViewUrl = value; }
        }


    }

    public class FormSection
    {
        public string Legend { get; set; }
        private string _ID = "section" + DateTime.Now.Ticks.ToString();
        public string ID
        {
            get { return _ID; }
            set { _ID = value; }
        }

        private bool _IsReadOnly = false;

        public bool IsReadOnly
        {
            get { return _IsReadOnly; }
            set { _IsReadOnly = value; }
        }

        private List<FormField> _Fields = new List<FormField>();
        public List<FormField> Fields
        {
            get { return _Fields; }
            set { _Fields = value; }
        }

        private string _CssFormSection = null;

        public string CssFormSection
        {
            get { return _CssFormSection ?? FormDefaults.Default.CssFormSection; }
            set { _CssFormSection = value; }
        }

        private SectionLayouts? _SectionLayout = null;
        public SectionLayouts SectionLayout
        {
            get { return _SectionLayout ?? FormDefaults.Default.SectionLayout; }
            set { _SectionLayout = value; }
        }

        private string _CssSectionTwoColumns = null;

        public string CssSectionTwoColumns
        {
            get { return _CssSectionTwoColumns ?? FormDefaults.Default.CssSectionTwoColumns; }
            set { _CssSectionTwoColumns = value; }
        }

        public FieldModel GetFieldModel(FormField Field, MetadataTable Model, RowData Data)
        {
            MetadataColumn mc = null;
            MetadataTable mt = Model;
            if (Field.ID.StartsWith("__"))
            {
                return new FieldModel(Field, null, Data.Column(Field.Name), SectionLayout);
            }
            if (Field.TableName != Model.Fullname)
            {
                mt = SqlBuilder.DefaultMetadata.FindTable(Field.TableName);
            }
            if (!mt.Columns.TryGetValue(Field.Name, out mc))
            {
                throw new InvalidOperationException("Cannot get a model for " + Field.Name);
            }

            return new FieldModel(Field, mc, Data.Column(Field.Alias ?? Field.Name), SectionLayout);

        }
    }

    public class LookupFormField : FormField
    {
        public LookupFormField()
        {
            base.FieldType = FieldTypes.LookupInput;
        }
        public override FieldTypes FieldType
        {
            get
            {
                return base.FieldType;
            }
            set
            {
                if (value == FieldTypes.LookupInput || value == FieldTypes.SelectList)
                {
                    base.FieldType = value;
                    //throw new ArgumentException("LookupField must be either LookupInput or SelectList", "FieldType");
                }

            }
        }

        private LookupSources _LookupSource = LookupSources.NameValueCollection;

        public LookupSources LookupSource
        {
            get { return _LookupSource; }
            set { _LookupSource = value; }
        }

        private NameValueCollection _Collection = null;

        public NameValueCollection Collection
        {
            get { return _Collection; }
            set { _Collection = value; }
        }

        private SqlBuilder _Builder = null;

        public SqlBuilder Builder
        {
            get { return _Builder; }
            set { _Builder = value; }
        }

        public string DisplayFieldID { get; set; }
        public string DisplayFieldName { get; set; }

        private string _CssLookupButtonClass = null;
        public string CssLookupButtonClass
        {
            get { return _CssLookupButtonClass ?? FormDefaults.Default.CssLookupButtonClass; }
            set { _CssLookupButtonClass = value; }
        }

        private string _CssLookupButtonClearClass = null;

        public string CssLookupButtonClearClass
        {
            get { return _CssLookupButtonClearClass ?? FormDefaults.Default.CssLookupButtonClearClass; }
            set { _CssLookupButtonClearClass = value; }
        }



    }

    public class FormField
    {
        public string ID { get; set; }
        public string Name { get; set; }

        public string DisplayName { get; set; }

        public string Alias { get; set; }
        public string TableName { get; set; }

        public string NullText { get; set; }

        private string _CssFieldGroup = null;

        public string ControlName
        {
            get
            {
                return (Alias ?? Name).Replace(" ", "_");
            }
        }

        public string CssFieldGroup
        {
            get
            {
                if (this.FieldType == FieldTypes.Checkbox)
                {
                    return (_CssCheckBoxLayout ?? FormDefaults.Default.CssCheckBoxLayout) + (this.IsReadOnly ? " disabled" : "");
                }
                else
                {
                    return _CssFieldGroup ?? FormDefaults.Default.CssFieldGroup;
                }
            }
            set { _CssFieldGroup = value; }
        }



        private string _CssLabelLayoutVertical = null;
        public string CssLabelLayoutVertical
        {
            get { return _CssLabelLayoutVertical; }
            set { _CssLabelLayoutVertical = value; }
        }
        private string _CssLabelLayoutHorizontalOneColumn = null;

        public string CssLabelLayoutHorizontalOneColumn
        {
            get { return _CssLabelLayoutHorizontalOneColumn; }
            set { _CssLabelLayoutHorizontalOneColumn = value; }
        }
        private string _CssLabelLayoutHorizontalTwoColumns = null;

        public string CssLabelLayoutHorizontalTwoColumns
        {
            get { return _CssLabelLayoutHorizontalTwoColumns; }
            set { _CssLabelLayoutHorizontalTwoColumns = value; }
        }

        public string GetCssLabelLayout(SectionLayouts SectionLayout)
        {
            switch (SectionLayout)
            {
                case SectionLayouts.Vertical:
                default:
                    return CssLabelLayoutVertical ?? FormDefaults.Default.CssLabelLayoutVertical;

                case SectionLayouts.HorizontalOneColumn:
                    return CssLabelLayoutHorizontalOneColumn ?? FormDefaults.Default.CssLabelLayoutHorizontalOneColumn;

                case SectionLayouts.HorizontalTwoColumns:
                    return CssLabelLayoutHorizontalTwoColumns ?? FormDefaults.Default.CssLabelLayoutHorizontalTwoColumns;

            }
        }

        private string _CssInputLayoutVertical = null;
        public string CssInputLayoutVertical
        {
            get { return _CssInputLayoutVertical; }
            set { _CssInputLayoutVertical = value; }
        }
        private string _CssInputLayoutHorizontalOneColumn = null;

        public string CssInputLayoutHorizontalOneColumn
        {
            get { return _CssInputLayoutHorizontalOneColumn; }
            set { _CssInputLayoutHorizontalOneColumn = value; }
        }
        private string _CssInputLayoutHorizontalTwoColumns = null;

        public string CssInputLayoutHorizontalTwoColumns
        {
            get { return _CssInputLayoutHorizontalTwoColumns; }
            set { _CssInputLayoutHorizontalTwoColumns = value; }
        }

        public string GetCssInputLayout(SectionLayouts SectionLayout)
        {
            switch (SectionLayout)
            {
                case SectionLayouts.Vertical:
                default:
                    return CssInputLayoutVertical ?? FormDefaults.Default.CssInputLayoutVertical;

                case SectionLayouts.HorizontalOneColumn:
                    return CssInputLayoutHorizontalOneColumn ?? FormDefaults.Default.CssInputLayoutHorizontalOneColumn;

                case SectionLayouts.HorizontalTwoColumns:
                    return CssInputLayoutHorizontalTwoColumns ?? FormDefaults.Default.CssInputLayoutHorizontalTwoColumns;

            }
        }


        private string _CssInputControlLayout = null;

        public string CssInputControlLayout
        {
            get { return _CssInputControlLayout ?? FormDefaults.Default.CssInputControlLayout; }
            set { _CssInputControlLayout = value; }
        }

        private string _CssCheckBoxLayout = null;

        public string CssCheckBoxLayout
        {
            get { return _CssCheckBoxLayout ?? FormDefaults.Default.CssCheckBoxLayout; }
            set { _CssCheckBoxLayout = value; }
        }

        private int? _MultiLineInputRows = null;

        public int MultiLineInputRows
        {
            get { return _MultiLineInputRows ?? FormDefaults.Default.MultiLineInputRows; }
            set { _MultiLineInputRows = value; }
        }



        private bool _IsHidden = false;

        public bool IsHidden
        {
            get { return _IsHidden; }
            set { _IsHidden = value; }
        }
        private bool _IsReadOnly = false;

        public bool IsReadOnly
        {
            get { return _IsReadOnly; }
            set { _IsReadOnly = value; }
        }

        private int? _MaxLength = null;

        public int? MaxLength
        {
            get { return _MaxLength; }
            set { _MaxLength = value; }
        }

        private bool _IsRequired = false;

        public bool IsRequired
        {
            get { return _IsRequired; }
            set { _IsRequired = value; }
        }

        private FieldTypes _FieldType = FieldTypes.Input;

        public virtual FieldTypes FieldType
        {
            get { return _FieldType; }
            set { _FieldType = value; }
        }
        private InputTypes _InputType = InputTypes.text;

        public InputTypes InputType
        {
            get { return _InputType; }
            set { _InputType = value; }
        }
    }

    public class FieldModel
    {
        internal FieldModel(FormField field, MetadataColumn model, object data, SectionLayouts sectionLayout)
        {
            this.Field = field;
            this.Model = model;
            this.Data = data;
            this.SectionLayout = sectionLayout;
        }
        public readonly FormField Field;
        public readonly MetadataColumn Model;
        public readonly object Data;
        public readonly SectionLayouts SectionLayout;
    }

    public class SectionModel
    {
        internal SectionModel(FormSection formSection, MetadataTable model, RowData data)
        {
            this.Section = formSection;
            this.Model = model;
            this.Data = data;
        }
        public readonly FormSection Section;
        public readonly MetadataTable Model;
        public readonly RowData Data;
    }


}
