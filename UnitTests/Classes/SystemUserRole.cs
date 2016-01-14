using System;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class SystemUserRole
{
		[Pk]
		public Decimal  SystemUserRoleID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		[Fk("SystemRole","SystemRoleID","dbo","RefSystemRole8")]
		public Decimal  SystemRoleID { get; set; }

		[Fk("SystemUser","SystemUserID","dbo","SystemUser_SystemUserRole_SystemUserID")]
		public Decimal  SystemUserID { get; set; }

	}
}
