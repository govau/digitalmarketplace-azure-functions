using System;

namespace Dta.Marketplace.Azure.Functions.Model {
    internal class VwRptMarketplaceBriefResponseDayRate {
        public string BriefCategory { get; set; }
        public double BriefResponseDayRate25PC { get; set; }
        public double BriefResponseDayRate50PC { get; set; }
        public double BriefResponseDayRate75PC { get; set; }
    }
}