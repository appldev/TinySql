namespace TinySql.Classes
{
    public partial class Contact
    {
        public Account ParentAccount { get; set; }

        public string AccountName { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
        public string Address3 { get; set; }
        public string PostalCode { get; set; }
        public string City { get; set; }
        

    }
}
