using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class UserSettings
{
		[Pk]
		public Decimal  UserSettingsID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public String  Culture { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		[Fk("SystemUser","SystemUserID","dbo","RefSystemUser129")]
		public Decimal  SystemUserID { get; set; }

		public String  Value { get; set; }

		public String  ValueKey { get; set; }

		public Nullable<Int32>  Version { get; set; }

	}
}
