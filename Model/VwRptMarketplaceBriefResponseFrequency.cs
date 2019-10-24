using System;

namespace Dta.Marketplace.Azure.Functions.Model {
    internal class VwRptMarketplaceBriefResponseFrequency {
        public int BriefID { get; set; }
        public string BriefCategory { get; set; }
        public string BriefType { get; set; }
        public string BriefPublishedDateFinancialYear { get; set; }
        public DateTime BriefPublishedDateMonthEnding { get; set; }
        public string BriefPublishedDateLatestMonthFlag { get; set; }
        public int NoOfResponses { get; set; }
        public string NoOfResponsesGroup { get; set; }
    }
}