#r "System.Data.SqlClient"

#load "query.csx"
#load "vwRptMarketplaceBrief.csx"
#load "../NameCount.csx"

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
    private readonly DateTime _now;

    public BriefQuery(DateTime now, string connectionString) : base(connectionString) {
        _now = now;
    }

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

    public async Task<dynamic> GetAggregationsAsync() {
        var briefs = await GetBriefsAsync();

        var topBuyers = briefs
            .Where(
                b => b.BriefPublishedDate.Year == _now.Year &&
                     b.BriefPublishedDate.Month == _now.Month
            )
            .GroupBy(b => b.BriefAgencyName,
                (key, b) => new NameCount {
                    Name = key,
                    Count = b.Count()
                }
            )
            .OrderByDescending(b => b.Count);

        var openToAllBrief = briefs
            .GroupBy(b => b.BriefOpenTo,
                (key, b) => new NameCount {
                    Name = key,
                    Count = b.Count()
                }
            );
        
        var openToAllBriefPercentage = openToAllBrief.Where(b => b.Name == "All").SingleOrDefault().Count /
            (decimal)openToAllBrief.Sum(b => b.Count) *
            100;

        var specialistBrief = briefs
            .GroupBy(b => b.BriefType,
                (key, b) => new NameCount {
                    Name = key,
                    Count = b.Count()
                }
            );
        
        var specialistBriefPercentage = specialistBrief.Where(b => b.Name == "Specialist").SingleOrDefault().Count /
            (decimal)specialistBrief.Sum(b => b.Count) *
            100;

        return new {
            openToAllBrief,
            openToAllBriefPercentage,
            specialistBrief,
            specialistBriefPercentage,
            topBuyers,
            topCategories = GetTopCategories(briefs),
            totalBriefs = briefs.Count(),
            totalBriefsThisMonth = GetTotalBriefsThisMonth(briefs)
        };
    }

    private IEnumerable<NameCount> GetTopCategories(List<VwRptMarketplaceBrief> briefs) {
        return briefs
            .Where(b => b.BriefCategory != "Not Specified")
            .GroupBy(b => b.BriefCategory, (key, b) => new NameCount {
                Name = key,
                Count = b.Count()
            })
            .OrderByDescending(b => b.Count)
            .Take(5);
    }
    private int GetTotalBriefsThisMonth(List<VwRptMarketplaceBrief> briefs) {
        return briefs
            .Where(
                b => b.BriefPublishedDate.Year == _now.Year &&
                     b.BriefPublishedDate.Month == _now.Month
            )
            .Count();
    }
}