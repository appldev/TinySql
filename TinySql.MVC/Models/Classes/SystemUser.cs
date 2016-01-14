using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class SystemUser
{
		[Pk]
		public Decimal  SystemUserID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public String  Efternavn { get; set; }

		public String  EMail { get; set; }

		public String  Fornavn { get; set; }

		public String  Initialer { get; set; }

		public String  Login { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public String  Navn { get; set; }

		[Fk("Organisation","OrganisationsID","dbo","RefOrganisation51")]
		public Decimal  OrganisationsID { get; set; }

		public String  Password { get; set; }

		public Nullable<Decimal>  StatusID { get; set; }

	}
}
