#r "System.Data.SqlClient"

#load "query.csx"
#load "vwRptAustenderContractSummary.csx"
#load "../NameCount.csx"

using System.Data.SqlClient;

internal class AustenderQuery : Query {

    private readonly string _austenderQuery = @"
select  [Contract Start Date (Financial Year)]
        ,[Contract Start Date (Month Ending)]
        ,[Contract Start Date Latest Month Flag]
        ,[Contract Start Date Latest FinYear Flag]
        ,[Contract Type]
        ,[Contract Type (Level 2)]
        ,[Contract Value Group]
        ,[Contract Spend Agency Name (short)]
        ,[Contract Supplier Name]
        ,[Contract Supplier Marketplace Name]
        ,[Contract Supplier SME Latest Status]
        ,[Contract Supplier Marketplace SME Status]
        ,[Contract SON ID]
        ,[SON Title (long)]
        ,[Total Contracts]
        ,[Total Contract Value Amount]
from    [Data].[VW_RPT_AusTender_Contract_Summary]
where	[Contract SON ID] in('SON3413842','SON3364729')
    ";
    private readonly DateTime _now;

    public AustenderQuery(DateTime now, string connectionString) : base(connectionString) {
        _now = now;
    }

    public async Task<List<VwRptAustenderContractSummary>> GetDataAsync() {
        return await base.ExecuteQueryAsync<VwRptAustenderContractSummary>(_austenderQuery, (reader) => {
            var item = new VwRptAustenderContractSummary {
                ContractStartDateFinancialYear = reader.GetString(0),
                ContractStartDateMonthEnding = reader.GetDateTime(1),
                ContractStartDateLatestMonthFlag = reader.GetString(2),
                ContractStartDateLatestFinYearFlag = reader.GetString(3),
                ContractType = reader.GetString(4),
                ContractTypeLevel2 = reader.GetString(5),
                ContractValueGroup = reader.GetString(6),
                ContractSpendAgencyNameShort = reader.GetString(7),
                ContractSupplierName = reader.GetString(8),
                ContractSupplierMarketplaceName = reader.GetString(9),
                ContractSupplierSMELatestStatus = reader.GetString(10),
                ContractSupplierMarketplaceSMEStatus = reader.GetString(11),
                ContractSONID = reader.GetString(12),
                SONTitleLong = reader.GetString(13),
                TotalContracts = reader.GetInt32(14),
                TotalContractValueAmount = reader.GetDouble(15)
            };
            return item;
        });
    }

    public async Task<dynamic> GetAggregationsAsync() {
        var data = await GetDataAsync();

        var smeValueAmount = data
            .Where(d => d.ContractSupplierMarketplaceSMEStatus == "SME")
            .Where(d => d.ContractStartDateMonthEnding.Date <= _now.Date)
            .Sum(a => a.TotalContractValueAmount);

        var totalValueAmount = data
            .Where(d => d.ContractStartDateMonthEnding.Date <= _now.Date)
            .Sum(a => a.TotalContractValueAmount);

        var smePercentage = smeValueAmount / totalValueAmount * 100;

        var totalValueAmountThisMonth = data
            .Where(d =>
                d.ContractStartDateMonthEnding.Year == _now.Year &&
                d.ContractStartDateMonthEnding.Month == _now.Month
            )
            .Sum(a => a.TotalContractValueAmount);


        var totalContractsThisMonth = data
            .Where(d =>
                d.ContractStartDateMonthEnding.Year == _now.Year &&
                d.ContractStartDateMonthEnding.Month == _now.Month
            )
            .Sum(d => d.TotalContracts);


        var smeContractsThisMonth = data
            .Where(d =>
                d.ContractStartDateMonthEnding.Year == _now.Year &&
                d.ContractStartDateMonthEnding.Month == _now.Month
            )
            .Where(d => d.ContractSupplierMarketplaceSMEStatus == "SME")
            .Sum(d => d.TotalContracts);

        var smeContractsPercentageThisMonth = smeContractsThisMonth / (double)totalContractsThisMonth * 100;

        var topSuppliersThisFinancialYear = data
            .Where(d =>
                d.ContractStartDateFinancialYear == GetFinancialYearString(_now)
            )
            .GroupBy(d => new {
                d.ContractSupplierMarketplaceName,
                d.ContractSupplierMarketplaceSMEStatus
            })
            .Select((g) => new {
                ContractSupplierMarketplaceName = g.Key.ContractSupplierMarketplaceName,
                ContractSupplierMarketplaceSMEStatus= g.Key.ContractSupplierMarketplaceSMEStatus,
                Count = g.Sum(d => d.TotalContracts)
            })
            .OrderByDescending(d => d.Count).ThenBy(d => d.ContractSupplierMarketplaceName)
            .Take(10);

        return new {
            smePercentage,
            smeContractsPercentageThisMonth,
            totalContractsThisMonth,
            topSuppliersThisFinancialYear,
            totalValueAmount,
            totalValueAmountThisMonth
        };
    }
}