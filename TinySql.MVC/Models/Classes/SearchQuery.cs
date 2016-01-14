using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class SearchQuery
{
		[Pk]
		public Decimal  SearchQueryID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public String  Entity { get; set; }

		public Decimal  IsSystem { get; set; }

		public String  Layout { get; set; }

		public String  MetaXML { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public Nullable<Decimal>  OwningBusinessUnitID { get; set; }

		public String  QueryName { get; set; }

		public String  TableClassName { get; set; }

	}
}
