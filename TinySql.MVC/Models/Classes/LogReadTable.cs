using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class LogReadTable
{
		[Pk]
		public Decimal  LogReadTableID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Decimal  EntityID { get; set; }

		public Decimal  PrimaryKey { get; set; }

		public Nullable<Decimal>  StatusID { get; set; }

	}
}
