using System;
using System.Threading.Tasks;
using Dta.Marketplace.Azure.Functions.Model;
using Dta.Marketplace.Azure.Functions.Query;
using Dta.Marketplace.Azure.Functions.Util;

namespace Dta.Marketplace.Azure.Functions.Business {
    internal class InsightBusiness {
        private readonly string _connectionString;
        public InsightBusiness(string connectionString) {
            _connectionString = connectionString;
        }
        public async Task<dynamic> GetResult(string monthEnding) {
            var now = StringUtil.GetMonthEnding(monthEnding);

            var agencyQuery = new AgencyQuery(_connectionString);
            var agencyData = await agencyQuery.GetAggregationsAsync();

            var briefResponseQuery = new BriefResponseQuery(now, _connectionString);
            var briefResponseData = await briefResponseQuery.GetAggregationsAsync();

            var briefQuery = new BriefQuery(now, _connectionString);
            var briefData = await briefQuery.GetAggregationsAsync();

            var austenderQuery = new AustenderQuery(now, _connectionString);
            var austenderData = await austenderQuery.GetAggregationsAsync();

            var supplierQuery = new SupplierQuery(now, _connectionString);
            var supplierData = await supplierQuery.GetAggregationsAsync();

            return new {
                agencyData,
                austenderData,
                briefResponseData,
                briefData,
                supplierData,
                thisMonth = now.ToString("MMMM yyyy")
            };
        }
    }
}