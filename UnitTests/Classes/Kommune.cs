using System;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class Kommune
{
		[Pk]
		public Decimal  KommuneID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Int32  Kommunekode { get; set; }

		public String  KommuneNavn { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		[Fk("Region","RegionID","dbo","Region_Kommune_RegionID")]
		public Decimal  RegionID { get; set; }

	}
}
