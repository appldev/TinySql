using System;
using System.Collections.Generic;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class Organisation
{
		[PK]
		public Decimal  OrganisationsID { get; set; }

		public String  Adresse1 { get; set; }

		public String  Adresse2 { get; set; }

		public String  Beskrivelse { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public String  DokumentFletteSti { get; set; }

		public String  Email { get; set; }

		[FK("Organisation","OrganisationsID","dbo","Organisation_Organisation_FaderOrganisationsID")]
		public Nullable<Decimal>  FaderOrganisationsID { get; set; }

		public String  Fax { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public String  Telefon { get; set; }

	}
}
