using System;
using System.Collections.Generic;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class Contact
{
		[PK]
		public Decimal  ContactID { get; set; }

		[FK("Account","AccountID","dbo","Account_AccountID_Contact_AcountID")]
		public Nullable<Decimal>  AccountID { get; set; }

		[FK("SystemUser","SystemUserID","dbo","FK_CreatedBy_SystemUser")]
		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Decimal  DatasourceID { get; set; }

		public Decimal  JobfunctionID { get; set; }

		public Decimal  JobpositionID { get; set; }

		public String  Mobile { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public String  Name { get; set; }

		public Decimal  OwningBusinessUnitID { get; set; }

		public Decimal  OwningUserID { get; set; }

		public String  PrivateEmail { get; set; }

		[FK("State","StateID","dbo","RefState125")]
		public Decimal  StateID { get; set; }

		public String  Telephone { get; set; }

		public String  Title { get; set; }

		public String  WorkEmail { get; set; }

	}
}
