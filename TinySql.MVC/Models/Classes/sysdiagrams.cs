using System;
using TinySql.Attributes;

namespace TinySql.MVC.Models
{	public partial class sysdiagrams
{
		[Pk]
		public Int32  diagram_id { get; set; }

		public Byte[]  definition { get; set; }

		public String  name { get; set; }

		public Int32  principal_id { get; set; }

		public Nullable<Int32>  version { get; set; }

	}
}
