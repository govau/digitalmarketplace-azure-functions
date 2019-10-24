using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dta.Marketplace.Azure.Functions.Model;

namespace Dta.Marketplace.Azure.Functions.Query {

    internal class ContractQuery : BaseQuery {

        private readonly string _query = @"
select [Contract Supplier Marketplace SME Status], sum([Total Contracts Value]) [Total Contracts Value], sum([Total Contracts]) [Total Contracts]
from (
	select	
		format ((select	MAX([Insert Timestamp])
			from	[Data].[VW_RPT_AusTender_Contract] ac
			where	ac.[Contract SON ID] in( 'SON3413842')),'yyyy-MM-dd') as [Last Updated],
		dr.[Report Date Effect To],
		ac.[Contract Start Date (Month Ending)],
			  ac.[Contract Start Date (Week Ending)],
		ac.[SON Title (long)],
		ac.[Contract SON ID],
		AC.[Contract Supplier Marketplace SME Status],
		COUNT(1) [Total Contracts],
		sum([Contract Value Amount]) [Total Contracts Value]
	from	[Data].[VW_RPT_AusTender_Contract] ac
		inner join 	(select	DATEADD(dd,-7, rd.Date_FY_Week_End_DT) as [Report Date Effect To] -- end of week (Sunday)
				from	Reference.Ref_Date rd
				where	rd.Date_DT = cast(getdate() as date)) dr on ac.[Contract Start Date] <= dr.[Report Date Effect To]
	where	ac.[Contract SON ID] in( 'SON3413842' ,'SON3364729' )
	group by	dr.[Report Date Effect To],
		ac.[Contract Start Date (Month Ending)],
			  ac.[Contract Start Date (Week Ending)],
		ac.[SON Title (long)],
		ac.[Contract SON ID],
		AC.[Contract Supplier Marketplace SME Status]
) r
group by r.[Contract Supplier Marketplace SME Status]
    ";

        public ContractQuery(string connectionString) : base(connectionString) { }

        public async Task<List<Contract>> GetAsync() {
            return await base.ExecuteQueryAsync<Contract>(_query, (reader) => {
                return new Contract {
                    SmeStatus = reader.GetString(0),
                    TotalValue = reader.GetDouble(1),
                    TotalContracts = reader.GetInt32(2)
                };
            });
        }

        public async Task<dynamic> GetAggregationsAsync() {
            var contracts = await GetAsync();

            var totalValue = default(double);
            var totalContracts = default(decimal);
            foreach (var r in contracts) {
                totalValue += r.TotalValue;
                totalContracts += r.TotalContracts;
            }
            var smeValue = contracts.Where(c => c.SmeStatus == "SME").SingleOrDefault();
            var smePercentage = smeValue.TotalContracts / totalContracts;

            return new {
                totalValue,
                smePercentage
            };
        }
    }
}