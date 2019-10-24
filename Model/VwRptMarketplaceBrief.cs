using System;

namespace Dta.Marketplace.Azure.Functions.Model {
    internal class VwRptMarketplaceBrief {
        public int BriefId { get; set; }
        public string BriefTitle { get; set; }
        public string BriefTypeLevel2 { get; set; }
        public string BriefType { get; set; }
        public string BriefCategory { get; set; }
        public string BriefOpenTo { get; set; }
        public DateTime BriefPublishedDate { get; set; }
        public string BriefPublishedDateFinancialYear { get; set; }
        public DateTime BriefPublishedDateMonthEnding { get; set; }
        public string MonthOfBriefPublishedDate { get; set; }
        public DateTime? BriefWithdrawnDate { get; set; }
        public string BriefAgencyName { get; set; }
        public string BriefPublishedDateLatestMonthFlag { get; set; }
        public string BriefPublishedDateLatestFinYearFlag { get; set; }
    }
}