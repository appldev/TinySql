using System;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class LogUpdateTable
{
		[Pk]
		public Decimal  AuditID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Nullable<Decimal>  EntityID { get; set; }

		public String  Field { get; set; }

		public String  FromValue { get; set; }

		public Nullable<Boolean>  IsUpdate { get; set; }

		public Nullable<Decimal>  PrimaryKey { get; set; }

		public Nullable<Decimal>  StatusID { get; set; }

		public String  ToValue { get; set; }

	}
}
