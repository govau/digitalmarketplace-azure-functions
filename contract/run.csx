#r "Newtonsoft.Json"
#r "System"
#r "System.Data.SqlClient"
#r "Microsoft.Extensions.Configuration.Abstractions"
#r "Microsoft.Extensions.Configuration"
#r "Microsoft.Extensions.Configuration.EnvironmentVariables"
#r "Microsoft.Extensions.Configuration.FileExtensions"
#r "Microsoft.Extensions.Configuration.Json"
#load "contract.csx"

using System.Net;
using System.Text;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Primitives;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

public static async Task<IActionResult> Run(HttpRequest req, ILogger log, ExecutionContext context) {
    log.LogInformation("C# HTTP trigger function processed a request.");

    var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();


    var connectionString = config.GetConnectionString("DevProcurementConnectionString");
    var sql = @"
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
    var contracts = new List<Contract>();
    using (var conn = new SqlConnection(connectionString))
    using (var cmd = new SqlCommand(sql, conn)) {
        conn.Open();

        using (var reader = await cmd.ExecuteReaderAsync()) {
            while (reader.Read()) {
                contracts.Add(new Contract {
                    SmeStatus = reader.GetString(0),
                    TotalValue = reader.GetDouble(1),
                    TotalContracts = reader.GetInt32(2)
                });
            }
        }
    }

    var totalValue = default(double);
    var totalContracts = default(decimal);
    foreach (var r in contracts) {
        totalValue += r.TotalValue;
        totalContracts += r.TotalContracts;
    }
    var smeValue = contracts.Where(c => c.SmeStatus == "SME").SingleOrDefault();
    var smePercentage = smeValue.TotalContracts / totalContracts;

    return  (ActionResult)new OkObjectResult($"{totalValue}\n{smePercentage}");
        //: new BadRequestObjectResult("Please pass a name on the query string or in the request body");
}
