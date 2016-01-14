using System;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class SystemPrivilege
{
		[Pk]
		public Decimal  SystemPrivilegeID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public String  Description { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

	}
}
