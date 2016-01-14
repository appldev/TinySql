using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class Eventlog
{
		[Pk]
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
