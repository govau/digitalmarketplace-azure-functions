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
using Dta.Marketplace.Azure.Functions.Query;

namespace Dta.Marketplace.Azure.Functions {
    public static class ContractHttpTrigger {
        [FunctionName("ContractHttpTrigger")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "get", Route = "contract")] HttpRequest req,
            ILogger log,ExecutionContext context) {
            var config = new ConfigurationBuilder()
                            .SetBasePath(context.FunctionAppDirectory)
                            .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                            .AddEnvironmentVariables()
                            .Build();

            var connectionString = config.GetConnectionString("DevProcurementConnectionString");

            var contractQuery = new ContractQuery(connectionString);
            var result = await contractQuery.GetAggregationsAsync();

            return new OkObjectResult(result);
        }
    }
}
