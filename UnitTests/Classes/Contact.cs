using System;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class Contact
{
		[Pk]
		public Decimal  ContactID { get; set; }

		[Fk("Account","AccountID","dbo","Account_AccountID_Contact_AcountID")]
		public Nullable<Decimal>  AccountID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Decimal  DatasourceID { get; set; }

		[Fk("Checkkode","CheckID","dbo","FK_Contact_JobfunctionID_Checkkode")]
		public Decimal  JobfunctionID { get; set; }

		[Fk("Checkkode","CheckID","dbo","FK_Contact_JobpositionID_Checkkode")]
		public Decimal  JobpositionID { get; set; }

		public String  Mobile { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public String  Name { get; set; }

		public Decimal  OwningBusinessUnitID { get; set; }

		public Decimal  OwningUserID { get; set; }

		public String  PrivateEmail { get; set; }

		[Fk("State","StateID","dbo","RefState125")]
		public Decimal  StateID { get; set; }

		public String  Telephone { get; set; }

		public String  Title { get; set; }

		public String  WorkEmail { get; set; }

	}
}
