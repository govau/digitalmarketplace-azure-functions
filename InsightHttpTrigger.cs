using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Microsoft.Extensions.Configuration;
using Dta.Marketplace.Azure.Functions.Business;

namespace Dta.Marketplace.Azure.Functions {
    public static class InsightHttpTrigger {
        [FunctionName("Insight Http Trigger")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "insight")] HttpRequest req,
            ILogger log, ExecutionContext context) {
            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var connectionString = config.GetConnectionString("DevProcurementConnectionString");

            var monthEnding = req.Query["monthEnding"];
            var insightBusiness = new InsightBusiness(connectionString);
            var result = await insightBusiness.GetResult(monthEnding);
            return new OkObjectResult(result);
        }
    }
}
