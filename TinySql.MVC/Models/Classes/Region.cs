using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class Region
{
		[Pk]
		public Decimal  RegionID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Int32  Landekode { get; set; }

		public String  LandeNavn { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

	}
}
