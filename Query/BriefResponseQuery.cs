using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dta.Marketplace.Azure.Functions.Model;

namespace Dta.Marketplace.Azure.Functions.Model {
    internal class BriefResponseQuery : BaseQuery {

        private readonly string _vwRptMarketplaceBriefResponseDayRateQuery = @"
SELECT [Brief Category]
      ,[Brief Response Day Rate (25PC)]
      ,[Brief Response Day Rate (50PC)]
      ,[Brief Response Day Rate (75PC)]
  FROM [Data].[VW_RPT_Marketplace_Brief_Response_DayRate]
    ";

        private readonly string _vwRptMarketplaceBriefResponseFrequencyQuery = @"
SELECT [Brief ID]
      ,[Brief Category]
      ,[Brief Type]
      ,[Brief Published Date (Financial Year)]
      ,[Brief Published Date (Month Ending)]
      ,[Brief Published Date Latest Month Flag]
      ,[No. of Responses]
      ,[No. of Responses Group]
  FROM [Data].[VW_RPT_Marketplace_Brief_Response_Frequency]
    ";

        private readonly DateTime _now;

        public BriefResponseQuery(DateTime now, string connectionString) : base(connectionString) {
            _now = now;
        }

        public async Task<List<VwRptMarketplaceBriefResponseDayRate>> GetVwRptMarketplaceBriefResponseDayRateDataAsync() {
            return await base.ExecuteQueryAsync<VwRptMarketplaceBriefResponseDayRate>(_vwRptMarketplaceBriefResponseDayRateQuery, (reader) => (
                new VwRptMarketplaceBriefResponseDayRate {
                    BriefCategory = GetFieldValueOrNull<string>(reader, 0),
                    BriefResponseDayRate25PC = GetFieldValueOrNull<double>(reader, 1),
                    BriefResponseDayRate50PC = GetFieldValueOrNull<double>(reader, 2),
                    BriefResponseDayRate75PC = GetFieldValueOrNull<double>(reader, 3)
                }
            ));
        }

        public async Task<List<VwRptMarketplaceBriefResponseFrequency>> GetVwRptMarketplaceBriefResponseFrequencyDataAsync() {
            return await base.ExecuteQueryAsync<VwRptMarketplaceBriefResponseFrequency>(_vwRptMarketplaceBriefResponseFrequencyQuery, (reader) => (
                new VwRptMarketplaceBriefResponseFrequency {
                    BriefID = GetFieldValueOrNull<int>(reader, 0),
                    BriefCategory = GetFieldValueOrNull<string>(reader, 1),
                    BriefType = GetFieldValueOrNull<string>(reader, 2),
                    BriefPublishedDateFinancialYear = GetFieldValueOrNull<string>(reader, 3),
                    BriefPublishedDateMonthEnding = GetFieldValueOrNull<DateTime>(reader, 4),
                    BriefPublishedDateLatestMonthFlag = GetFieldValueOrNull<string>(reader, 5),
                    NoOfResponses = GetFieldValueOrNull<int>(reader, 6),
                    NoOfResponsesGroup = GetFieldValueOrNull<string>(reader, 7)
                }
            ));
        }

        public async Task<dynamic> GetAggregationsAsync() {
            var dailyRatesData = await GetVwRptMarketplaceBriefResponseDayRateDataAsync();
            var dailyRates = dailyRatesData.OrderBy(d => d.BriefCategory);

            var vwRptMarketplaceBriefResponseFrequencyData = await GetVwRptMarketplaceBriefResponseFrequencyDataAsync();
            var responsesPerOpportunity = vwRptMarketplaceBriefResponseFrequencyData
                .GroupBy(d => new {
                    d.NoOfResponsesGroup,
                    d.BriefType
                })
                .Select(g => new {
                    NoOfResponses = g.Key.NoOfResponsesGroup,
                    BriefType = g.Key.BriefType,
                    Count = g.Count()
                })
                .OrderBy(d => d.NoOfResponses);

            return new {
                dailyRates,
                responsesPerOpportunity
            };
        }
    }
}