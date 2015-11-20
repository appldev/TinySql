using System;
using System.Collections.Generic;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class Activity
{
		[PK]
		public Decimal  ActivityID { get; set; }

		public Decimal  ActivityStatusID { get; set; }

		public Decimal  ActivityTypeID { get; set; }

		[FK("Contact","ContactID","dbo","Contact_Activity_ContactID")]
		public Decimal  ContactID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public DateTime  Date { get; set; }

		public String  Description { get; set; }

		public Nullable<Int32>  DurationMinutes { get; set; }

		public Boolean  HasAttachment { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public Decimal  OwningBusinessUnitID { get; set; }

		public Decimal  OwningUserID { get; set; }

		public String  Title { get; set; }

	}
}
