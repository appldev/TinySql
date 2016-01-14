using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class Listmember
{
		[Pk]
		public Decimal  ListmemberID { get; set; }

		[Fk("Contact","ContactID","dbo","Contact_Listmember_ContactID")]
		public Decimal  ContactID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Nullable<DateTime>  LastUpdated { get; set; }

		[Fk("List","ListID","dbo","List_Listmember_ListID")]
		public Decimal  ListID { get; set; }

		public Decimal  ListmemberStatusID { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public Decimal  OwningBusinessUnitID { get; set; }

		public Decimal  OwningUserID { get; set; }

	}
}
