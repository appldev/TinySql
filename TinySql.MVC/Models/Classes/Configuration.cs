using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class Configuration
{
		[Pk]
		public String  ConfigName { get; set; }

		public String  ConfigValue { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

	}
}
