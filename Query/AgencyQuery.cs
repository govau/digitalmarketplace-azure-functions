using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dta.Marketplace.Azure.Functions.Model;

namespace Dta.Marketplace.Azure.Functions.Query {

    internal class AgencyQuery : BaseQuery {

        private readonly string _agencyQuery = @"
SELECT [Agency Name]
      ,[Agency Type of Body]
      ,[Agency Commonwealth Flag]
FROM [Data].[VW_RPT_Marketplace_Agency]
    ";

        public AgencyQuery(string connectionString) : base(connectionString) { }

        public async Task<List<VwRptMarketplaceAgency>> GetAgenciesAsync() {
            return await base.ExecuteQueryAsync<VwRptMarketplaceAgency>(_agencyQuery, (reader) => {
                return new VwRptMarketplaceAgency {
                    AgencyName = reader.GetString(0),
                    AgencyTypeOfBody = reader.GetString(1),
                    AgencyCommonwealthFlag = reader.GetString(2)
                };
            });
        }

        public async Task<dynamic> GetAggregationsAsync() {
            var vwRptMarketplaceAgencies = await GetAgenciesAsync();

            var agencyTypeCounts = vwRptMarketplaceAgencies.GroupBy(
                a => a.AgencyTypeOfBody,
                (key, e) => new NameCount {
                    Name = key,
                    Count = e.Count()
                })
                .OrderBy(d => d.Name);

            var agencyCommonwealthCounts = vwRptMarketplaceAgencies.GroupBy(
                a => a.AgencyCommonwealthFlag,
                (key, e) => new NameCount {
                    Name = key,
                    Count = e.Count()
                });

            var commonwealthPercent =
                agencyCommonwealthCounts.Where(a => a.Name == "Y").SingleOrDefault()?.Count /
                (decimal)agencyCommonwealthCounts.Sum(a => a.Count);

            return new {
                agencyTypeCounts,
                commonwealthPercent
            };
        }
    }
}