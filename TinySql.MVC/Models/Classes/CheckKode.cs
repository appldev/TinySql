using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class CheckKode
{
		[Pk]
		public Decimal  CheckGroup { get; set; }

		[Pk]
		public Decimal  CheckID { get; set; }

		public String  BeskrivelseDK { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Nullable<Int32>  ImageIndex { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public Nullable<Decimal>  SortOrder { get; set; }

		public Nullable<Decimal>  StatusID { get; set; }

		public String  XtraInfo { get; set; }

	}
}
