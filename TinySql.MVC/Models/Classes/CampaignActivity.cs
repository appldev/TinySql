using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class CampaignActivity
{
		[Pk]
		public Decimal  CampaignActivityID { get; set; }

		public Decimal  CampaignActivityTypeID { get; set; }

		[Fk("Campaign","CampaignID","dbo","Campaign_CampaignActivity_CampaignID")]
		public Decimal  CampaignID { get; set; }

		[Fk("Contact","ContactID","dbo","Contact_CampaignActivity_ContactID")]
		public Decimal  ContactID { get; set; }

		public Nullable<Int32>  Count { get; set; }

		public Decimal  CreatedBy { get; set; }

		public DateTime  CreatedOn { get; set; }

		public String  Description { get; set; }

		public Decimal  ModifiedBy { get; set; }

		public DateTime  ModifiedOn { get; set; }

		public Decimal  OwningBusinessUnitID { get; set; }

		public Decimal  OwningUserID { get; set; }

		public DateTime  RegisteredOn { get; set; }

		public String  Url { get; set; }

	}
}
