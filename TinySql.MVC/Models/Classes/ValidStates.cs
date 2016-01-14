using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class ValidStates
{
		[Pk]
		public Decimal  ValidStatesID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Decimal  EntityID { get; set; }

		[Fk("State","StateID","dbo","State_ValidStates_FromStateID")]
		public Decimal  FromStateID { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		[Fk("State","StateID","dbo","RefState126")]
		public Decimal  ToStateID { get; set; }

	}
}
