using System;
using System.Collections.Generic;
using TinySql.Attributes;

namespace TinySql.Classes
{	public partial class Attachment
{
		[PK]
		public Decimal  AttachmentID { get; set; }

		[FK("Activity","ActivityID","dbo","Activity_ActivityID_Attachment_ActivityID")]
		public Nullable<Decimal>  ActivityID { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public String  DocumentValue { get; set; }

		public String  FileName { get; set; }

		public String  MimeType { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public Decimal  OwningBusinessUnitID { get; set; }

		public Decimal  OwningUserID { get; set; }

	}
}
