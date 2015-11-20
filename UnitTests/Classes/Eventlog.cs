using System;
using System.Collections.Generic;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class Eventlog
{
		[PK]
		public Decimal  EventLogID { get; set; }

		public Decimal  CategoryID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public String  CustomData { get; set; }

		public String  Description { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public Decimal  SourceID { get; set; }

		public String  Title { get; set; }

	}
}
