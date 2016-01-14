using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using TinySql.Attributes;
using TinySql.Classes;

namespace UnitTests
{
    [TestClass]
    public class BaseTest
    {
        [TestInitialize]
        public void Initialize()
        {
            Assert.AreEqual<bool>(SetupData.Setup(), true);
            TinySql.Cache.CacheProvider.UseResultCache = true;
            TinySql.Cache.CacheProvider.ResultCache.CacheMinutes = 1;
        }
        public BaseTest()
        {
            
        }



    }


    


    public class Account
    {
        public decimal AccountID { get; set; }
        public string Name { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
        public string Address3 { get; set; }
        public string PostalCode { get; set; }
        public string City { get; set; }

        public string Telephone { get; set; }
        public string Telefax { get; set; }
        public string Web { get; set; }

        public decimal AccountTypeID{ get; set; }

        [Fk("State", toSchema: "")]
        public decimal StateID { get; set; }

        public string State { get; set; }

        public decimal DatasourceID { get; set; }

        public string Datasource { get; set; }

        public decimal CreatedBy { get; set; }
        public decimal ModifiedBy { get; set; }

        public DateTime CreatedOn { get; set; }
        public DateTime ModifiedOn { get; set; }

        public decimal OwningUserID { get; set; }
        public decimal OwningBusinessUnitID { get; set; }

        public List<Contact> Contacts { get; set; }





        
    }


    [Serializable()]
    public class Shopper
    {
        /// <summary>
        /// All fields related to the Customer table are in the Customer instance.</summary>
        public object Customer { get; internal set; }

        public string ShopperReference { get; set; }
        public decimal ShopperLocalID { get; set; }

        public string Company { get; set; }
        public string Name { get; set; }
        public string Address1 { get; set; }
        public string Address2 { get; set; }
        public string Login { get; set; }
        public string Password { get; set; }
        public string ZipCode { get; set; }
        public string City { get; set; }

        public string Email { get; set; }
        public string Phone { get; set; }
        public string Mobile { get; set; }
        public string CVRNumber { get; set; }
        public object State { get; set; }
        public decimal? AttentionPersonID { get; set; }

        public bool NoQuestions { get; set; }
        public object ShopperType { get; set; }
        public string CompanyFax { get; set; }
        public string CompanyPhone { get; set; }
        public string CompanyEMail { get; set; }
        public string Title { get; set; }

        public object UserType { get; set; }
        public string Country { get; set; }
        public bool BonusReceived { get; set; }
        public decimal BonusForEmails { get; set; }
        public int? BonusDaysForEmails { get; set; }
        public DateTime? BonusDateForEmails { get; set; }
        public int CurrencyCode { get; set; }


        private string currencyCodeDescription;
        public string CurrencyCodeDescription
        {
            get
            {
                //if (currencyCodeDescription == null)
                //    currencyCodeDescription = Calc.CurrencyList.ItemOrDefault(CurrencyCode).Description;
                return currencyCodeDescription;
            }
        }


        /// <summary>
        /// Note: Not a column in table Customer (most possible from sproc).</summary>
        public bool ShowBonus { get; set; }
        public bool NewsEmail { get; set; }

        private bool bonusAvailableFetched { get; set; }
        private decimal bonusAvailable;

        public decimal BonusAvailable
        {
            get
            {
                if (ShowBonus)
                {
                    if (bonusAvailableFetched)
                        return bonusAvailable;
                    else
                    {
                        lock (this)
                        {
                            if (!bonusAvailableFetched)
                            {
                                //bonusAvailable = Convert.ToDecimal(PhilipsonWine.datalayer.Shopper.cShopper.GetCurrentShopperBonus(this.ShopperReference).GetValueOrDefault());
                                bonusAvailableFetched = true;
                            }
                        }
                    }
                }
                return bonusAvailable;
            }
        }

        public void RequeryBonus()
        {
            bonusAvailableFetched = false;
        }

        

        public Shopper()
        {
            //State = PhilipsonWineConfiguration.EShopperState.NewShopper;
            //ShopperType = PhilipsonWineConfiguration.EShopperType.Unknown;
            //UserType = PhilipsonWineConfiguration.EUserType.NormalUser;
        }

        

        /// <summary>
        /// Determines if a shopper pattern type, used by bonus, vouchers, campaigns and ordergifts
        /// matches a specific shopper type.
        /// </summary>
        /// <param name="patternShopperType">The pattern of a shoppertype (1 to 3) and 4 for all.</param>
        /// <param name="shopperType">The shopper type of the shopper</param>
        /// <returns>true, if the shopper matches the pattern, else false.</returns>
        public static bool MatchesShopper(int patternShopperType, int shopperType)
        {
            return patternShopperType == shopperType ||
                (patternShopperType == 4 && (shopperType == 2 || shopperType == 3)) ||
                patternShopperType == 0;
        }


    }


}
