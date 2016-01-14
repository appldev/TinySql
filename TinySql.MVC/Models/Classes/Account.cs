using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class Account
{
		[Pk]
		public Decimal  AccountID { get; set; }

		public Decimal  AccountTypeID { get; set; }

		public String  Address1 { get; set; }

		public String  Address2 { get; set; }

		public String  Address3 { get; set; }

		public String  City { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Decimal  DatasourceID { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public String  Name { get; set; }

		public Decimal  OwningBusinessUnitID { get; set; }

		public Decimal  OwningUserID { get; set; }

		public String  PostalCode { get; set; }

		[Fk("State","StateID","dbo","State_Account_StateID")]
		public Decimal  StateID { get; set; }

		public String  Telefax { get; set; }

		public String  Telephone { get; set; }

		public String  Web { get; set; }

	}
}
