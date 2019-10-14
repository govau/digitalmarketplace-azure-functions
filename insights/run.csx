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
#load "austenderQuery.csx"
#load "briefResponseQuery.csx"
#load "briefQuery.csx"
#load "supplierQuery.csx"

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
    
    var monthEnding = req.Query["monthEnding"];
    var now = DateTime.Now;
    now = new DateTime(now.Year, now.Month, DateTime.DaysInMonth(now.Year, now.Month));
    if (string.IsNullOrWhiteSpace(monthEnding) == false) {
        DateTime.TryParse(monthEnding, out now);
        now = new DateTime(now.Year, now.Month, DateTime.DaysInMonth(now.Year, now.Month));
    }

    var agencyQuery = new AgencyQuery(connectionString);
    var agencyData = await agencyQuery.GetAggregationsAsync();

    var briefResponseQuery = new BriefResponseQuery(now, connectionString);
    var briefResponseData = await briefResponseQuery.GetAggregationsAsync();

    var briefQuery = new BriefQuery(now, connectionString);
    var briefData = await briefQuery.GetAggregationsAsync();

    var austenderQuery = new AustenderQuery(now, connectionString);
    var austenderData = await austenderQuery.GetAggregationsAsync();

    var supplierQuery = new SupplierQuery(now, connectionString);
    var supplierData = await supplierQuery.GetAggregationsAsync();

    var result = new {
        agencyData,
        austenderData,
        briefResponseData,
        briefData,
        supplierData,
        thisMonth = now.ToString("MMMM yyyy")
    };
    return new OkObjectResult(result);
}
