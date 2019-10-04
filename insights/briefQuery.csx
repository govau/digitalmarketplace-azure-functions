#r "System.Data.SqlClient"

#load "query.csx"
#load "vwRptMarketplaceBrief.csx"

using System.Data.SqlClient;

internal class BriefQuery : Query {

    private readonly string _briefQuery = @"
select	mb.*,
	case when dr.Row_FY_NR_desc = 1 and rd.Date_FY_Month_NR = dr.Date_FY_Month_NR then 'Y' else 'N' end [Brief Published Date Latest Month Flag],
	case when dr.Row_FY_NR_desc = 1 then 'Y' else 'N' end [Brief Published Date Latest FinYear Flag]
from	[Data].[VW_RPT_Marketplace_Brief] mb
	inner join [Reference].Ref_Date rd on mb.[Brief Published Date] = rd.Date_DT
	inner join [Reference].[VW_Ref_Date_Range_EOM]  dr on rd.Date_FY_NR = dr.Date_FY_NR 
					         and rd.Date_FY_Month_NR <= case when dr.Row_FY_NR_desc = 1 then dr.Date_FY_Month_NR else 12 end
    ";

    public BriefQuery(string connectionString) : base(connectionString) { }

    public async Task<List<VwRptMarketplaceBrief>> GetBriefsAsync() {
        return await base.ExecuteQueryAsync<VwRptMarketplaceBrief>(_briefQuery, (reader) => {
            var item = new VwRptMarketplaceBrief {
                BriefId = reader.GetInt32(0),
                BriefTitle = reader.GetString(1),
                BriefTypeLevel2 = reader.GetString(2),
                BriefType = reader.GetString(3),
                BriefCategory = reader.GetString(4),
                BriefOpenTo = reader.GetString(5),
                BriefPublishedDate = reader.GetDateTime(6),
                BriefPublishedDateFinancialYear = reader.GetString(7),
                BriefPublishedDateMonthEnding = reader.GetDateTime(8),
                MonthOfBriefPublishedDate = reader.GetString(9),

                BriefAgencyName = reader.GetString(11),
                BriefPublishedDateLatestMonthFlag = reader.GetString(12),
                BriefPublishedDateLatestFinYearFlag = reader.GetString(13)
            };
            if (reader.IsDBNull(10) == false) {
                item.BriefWithdrawnDate = reader.GetDateTime(10);
            }
            return item;
        });
    }

    public async Task<dynamic> GetAggregationsAsync(DateTime now) {
        var briefs = await GetBriefsAsync();

        var topBuyers = briefs
            .Where(
                b => b.BriefPublishedDate.Year == now.Year &&
                b.BriefPublishedDate.Month == now.Month
            )
            .GroupBy(b => b.BriefAgencyName, (key, b) => {
                return new {
                    BriefAgencyName = key,
                    BriefCount = b.Count()
                };
            })
            .OrderByDescending(b => b.BriefCount);

        var topCategories = briefs
            .Where(b => b.BriefCategory != "Not Specified")
            .GroupBy(b => b.BriefCategory, (key, b) => {
                return new {
                    BriefCategory = key,
                    BriefCategoryCount = b.Count()
                };
            })
            .OrderByDescending(b => b.BriefCategoryCount);

        return new {
            topBuyers,
            topCategories
        };
    }
}