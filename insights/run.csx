#r "Newtonsoft.Json"
#r "System"
#r "System.Data.SqlClient"
#r "Microsoft.Extensions.Configuration.Abstractions"
#r "Microsoft.Extensions.Configuration"
#r "Microsoft.Extensions.Configuration.EnvironmentVariables"
#r "Microsoft.Extensions.Configuration.FileExtensions"
#r "Microsoft.Extensions.Configuration.Json"
#load "vwRptMarketplaceAgency.csx"
#load "agencyQuery.csx"
#load "briefQuery.csx"

using System;
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
    
    var queryNow = req.Query["now"];
    var now = DateTime.Now;
    if (string.IsNullOrWhiteSpace(queryNow) == false) {
        DateTime.TryParse(queryNow, out now);
    }
    

    var agencyQuery = new AgencyQuery(connectionString);
    var agencyData = await agencyQuery.GetAggregationsAsync();

    var briefQuery = new BriefQuery(connectionString);
    var briefData = await briefQuery.GetAggregationsAsync(now);

    var result = new {
        agencyData,
        briefData
    };
    return new OkObjectResult(result);
}
