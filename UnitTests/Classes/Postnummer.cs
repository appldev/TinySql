using System;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class Postnummer
{
		[Pk]
		public Decimal  Postnr { get; set; }

		public String  Bynavn { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		[Fk("Kommune","KommuneID","dbo","Kommune_Postnummer_KommuneID")]
		public Decimal  KommuneID { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

	}
}
