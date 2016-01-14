using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class List
{
		[Pk]
		public Decimal  ListID { get; set; }

		public String  CMID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Nullable<DateTime>  LastUpdated { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public String  Name { get; set; }

		public Decimal  OwningBusinessUnitID { get; set; }

		public Decimal  OwningUserID { get; set; }

	}
}
