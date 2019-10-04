#r "System.Data.SqlClient"

#load "query.csx"
#load "vwRptMarketplaceAgency.csx"

using System.Data.SqlClient;

internal class AgencyQuery : Query {

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
            (key, e) => new {
                AgencyTypeOfBody = key,
                AgencyTypeOfBodyCount = e.Count()
            });

        var agencyCommonwealthCounts = vwRptMarketplaceAgencies.GroupBy(
            a => a.AgencyCommonwealthFlag,
            (key, e) => new {
                AgencyCommonwealthFlag = key,
                AgencyCommonwealthFlagCount = e.Count()
            });

        var commonwealthPercent =
            agencyCommonwealthCounts.Where(a => a.AgencyCommonwealthFlag == "Y").SingleOrDefault()?.AgencyCommonwealthFlagCount /
            (decimal)agencyCommonwealthCounts.Sum(a => a.AgencyCommonwealthFlagCount) *
            100;

        return new {
            agencyTypeCounts,
            commonwealthPercent
        };
    }
}