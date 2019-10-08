#r "System.Data.SqlClient"

#load "query.csx"
#load "vwRptMarketplaceBrief.csx"
#load "../NameCount.csx"

using System.Data.SqlClient;

internal class BriefQuery : Query {

    private readonly string _query = @"
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

    public async Task<List<VwRptMarketplaceBrief>> GetDataAsync() {
        return await base.ExecuteQueryAsync<VwRptMarketplaceBrief>(_query, (reader) => (
            new VwRptMarketplaceBrief {
                BriefId = GetFieldValueOrNull<int>(reader, 0),
                BriefTitle = GetFieldValueOrNull<string>(reader, 1),
                BriefTypeLevel2 = GetFieldValueOrNull<string>(reader, 2),
                BriefType = GetFieldValueOrNull<string>(reader, 3),
                BriefCategory = GetFieldValueOrNull<string>(reader, 4),
                BriefOpenTo = GetFieldValueOrNull<string>(reader, 5),
                BriefPublishedDate = GetFieldValueOrNull<DateTime>(reader, 6),
                BriefPublishedDateFinancialYear = GetFieldValueOrNull<string>(reader, 7),
                BriefPublishedDateMonthEnding = GetFieldValueOrNull<DateTime>(reader, 8),
                MonthOfBriefPublishedDate = GetFieldValueOrNull<string>(reader, 9),
                BriefWithdrawnDate = GetFieldValueOrNull<DateTime>(reader, 10),
                BriefAgencyName = GetFieldValueOrNull<string>(reader, 11),
                BriefPublishedDateLatestMonthFlag = GetFieldValueOrNull<string>(reader, 12),
                BriefPublishedDateLatestFinYearFlag = GetFieldValueOrNull<string>(reader, 13)
            }
        ));
    }

    public async Task<dynamic> GetAggregationsAsync() {
        var data = await GetDataAsync();

        var topBuyersThisMonth = data
            .Where(
                d => d.BriefPublishedDate.Year == _now.Year &&
                     d.BriefPublishedDate.Month == _now.Month
            )
            .GroupBy(d => d.BriefAgencyName,
                (key, b) => new NameCount {
                    Name = key,
                    Count = b.Count()
                }
            )
            .OrderByDescending(d => d.Count)
            .Take(5);

        var openToAllBrief = data
            .Where(d => d.BriefPublishedDate.Date <= _now.Date)
            .GroupBy(b => b.BriefOpenTo,
                (key, b) => new NameCount {
                    Name = key,
                    Count = b.Count()
                }
            );
        
        var openToAllBriefPercentage = openToAllBrief.Where(b => b.Name == "All").SingleOrDefault().Count /
            (decimal)openToAllBrief.Sum(b => b.Count) *
            100;

        var specialistBrief = data
            .Where(d => d.BriefPublishedDate.Date <= _now.Date)
            .GroupBy(b => b.BriefType,
                (key, b) => new NameCount {
                    Name = key,
                    Count = b.Count()
                }
            );
        
        var specialistBriefPercentage = specialistBrief.Where(b => b.Name == "Specialist").SingleOrDefault().Count /
            (decimal)specialistBrief.Sum(b => b.Count) *
            100;

        var totalBriefs = data
            .Where(d => d.BriefPublishedDate.Date <= _now.Date)
            .Count();

        return new {
            openToAllBrief,
            openToAllBriefPercentage,
            specialistBrief,
            specialistBriefPercentage,
            topBuyersThisMonth,
            topCategories = GetTopCategories(data),
            totalBriefs,
            totalBriefsThisMonth = GetTotalBriefsThisMonth(data)
        };
    }

    private IEnumerable<NameCount> GetTopCategories(List<VwRptMarketplaceBrief> data) {
        return data
            .Where(d => d.BriefCategory != "Not Specified")
            .Where(d => d.BriefPublishedDate.Date <= _now.Date)
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