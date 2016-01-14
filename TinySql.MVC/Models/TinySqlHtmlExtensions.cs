using System;
using System.Web.Mvc;

namespace TinySql.UI
{
    public static class TinySqlExtensions
    {
        public static MvcHtmlString RegisterTinySqlPage(this HtmlHelper helper)
        {
            string html = "<div id=\"tinysql\"></div>";
            return MvcHtmlString.Create(html);
        }
        public static MvcHtmlString TinySqlLabel(this HtmlHelper helper, FieldModel model)
        {

            if (model.Field.FieldType != FieldTypes.Checkbox && model.Field.FieldType != FieldTypes.Option)
            {
                string label = string.Format("<label id=\"label{1}\" for=\"{1}\" class=\"{2}\">{0}</label>",
                model.Field.DisplayName,                      // 0
                model.Field.ID,                               // 1
                model.Field.GetCssLabelLayout(model.SectionLayout)  // 2
                );
                return MvcHtmlString.Create(label);
            }
            return MvcHtmlString.Empty;
        }

        public static MvcHtmlString TinySqlInput(this HtmlHelper helper, FieldModel model)
        {
            string ctrl = "";
            string ReadOnly = model.Field.IsReadOnly ? "readonly" : "";
            if (model.Field.IsHidden)
            {
                ctrl = string.Format("<input type=\"hidden\" name=\"{0}\" value=\"{1}\" >",
                    model.Field.ControlName,                                       // 0
                    model.Data == null ? "" : Convert.ToString(model.Data)  // 1
                    );
            }
            else if (model.Field.FieldType == FieldTypes.TextArea)
            {
                ctrl = string.Format("<textarea id=\"{0}\" name=\"{1}\" class=\"{2}\" placeholder=\"{3}\" rows=\"{4}\" {7} {5}>{6}</textarea>",
                    model.Field.ID,
                    model.Field.ControlName,
                    model.Field.CssInputControlLayout,
                    model.Field.NullText,
                    model.Field.MultiLineInputRows,
                    model.Field.IsReadOnly ? "disabled": "",
                    model.Data != null ? Convert.ToString(model.Data) : "",
                    model.Field.IsRequired && !model.Field.IsReadOnly ? "required" : ""
                    );
            }
            else if (model.Field.FieldType == FieldTypes.Checkbox)
            {
                bool b;
                if (bool.TryParse(model.Data.ToString(), out b))
                {
                    ctrl = string.Format("<label><input type=\"checkbox\" value=\"true\" name=\"{1}\" id=\"input{0}\" {2} > {3}</label>",
                        model.Field.ID,
                        model.Field.ControlName,
                        // model.Data == null ? "" : Convert.ToString(model.Data),
                        b ? "checked" : "",
                        model.Field.DisplayName,
                        model.Field.CssCheckBoxLayout,
                        model.Field.IsReadOnly ? "disabled" : ""
                        );
                }
                else
                {
                    throw new ArgumentException("Non boolean value specified for the form field " + model.Field.Name, "model");
                }
            }
            else if (model.Field.FieldType == FieldTypes.Input)
            {
                ctrl = string.Format("<input type=\"{4}\" class=\"{2}\" name=\"{1}\" id=\"input{0}\" placeholder=\"Enter {7}\" value=\"{5}\" {6} >",
                    model.Field.ID,                                         // 0
                    //model.Field.TableName + "_" + model.Field.Name,                                       // 1
                    model.Field.Alias ?? model.Field.Name,                                       // 1
                    model.Field.CssInputControlLayout,                      // 2
                    model.Field.NullText,                                   // 3
                    model.Field.InputType.ToString().Replace("_", "-"),      // 4
                    model.Data == null ? "" : Convert.ToString(model.Data),  // 5
                    ReadOnly,                                                // 6
                    model.Field.DisplayName
                    );
            }
            else if (model.Field.FieldType == FieldTypes.LookupInput || model.Field.FieldType == FieldTypes.SelectList)
            {
                LookupFormField lookup = (model.Field as LookupFormField);
                ResultTable results = null;
                if (lookup.FieldType == FieldTypes.SelectList)
                {
                    string ctrlItems = "";
                    string v = model.Data != null ? Convert.ToString(model.Data) : "";
                    if (lookup.LookupSource == LookupSources.NameValueCollection)
                    {
                        foreach (string name in lookup.Collection.AllKeys)
                        {
                            string value = lookup.Collection[name];
                            ctrlItems += string.Format("<option value=\"{0}\" {1}>{2}</option>",
                                value,
                                v.Equals(value) ? "selected" : "",
                                name);
                        }
                    }
                    else if (lookup.LookupSource == LookupSources.SqlBuilder)
                    {
                        results = lookup.Builder.Execute();
                        foreach (RowData row in results)
                        {
                            string name = Convert.ToString(row.Column(row.Columns[0]));
                            string value = Convert.ToString(row.Column(row.Columns[1]));
                            ctrlItems += string.Format("<option value=\"{0}\" {1}>{2}</option>",
                                value,
                                v.Equals(value) ? "selected" : "",
                                name);
                        }
                    }
                    ctrl = string.Format("<select id=\"{0}\" name=\"{1}\" class=\"{2}\" {3} >{4}</select>",
                    lookup.ID,                      // 0
                    lookup.Alias ?? lookup.Name,                                       // 1
                        //lookup.TableName + "_" + lookup.Name,                    // 1
                    lookup.CssInputControlLayout,   // 2
                    ReadOnly,                       // 3
                    ctrlItems                       // 4  
                    );
                }
                else if (lookup.FieldType == FieldTypes.LookupInput)
                {
                    throw new NotImplementedException();
                }
            }


            return MvcHtmlString.Create(ctrl);
        }
    }
}