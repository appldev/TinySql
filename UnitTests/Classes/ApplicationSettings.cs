using System;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class ApplicationSettings
{
		[Pk]
		public Decimal  ApplicationSettingsID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public String  Culture { get; set; }

		public Boolean  IsPublished { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public String  Value { get; set; }

		public String  ValueKey { get; set; }

		public Nullable<Int32>  Version { get; set; }

	}
}
