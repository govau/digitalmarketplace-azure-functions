using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Text;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Primitives;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Dta.Marketplace.Azure.Functions.Business;
using Dta.Marketplace.Azure.Functions.Util;

namespace Dta.Marketplace.Azure.Functions {
    public static class InsightUploadHttpTrigger {
        [FunctionName("InsightUploadHttpTrigger")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = "insight-upload")] HttpRequest req,
            ILogger log, ExecutionContext context) {
            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var connectionString = config.GetConnectionString("DevProcurementConnectionString");
            var dmApiUrl = Environment.GetEnvironmentVariable("dmApiUrl");
            var dmApiKey = Environment.GetEnvironmentVariable("dmApiKey");

            var monthEnding = req.Query["monthEnding"];
            var insightBusiness = new InsightBusiness(connectionString);
            var result = await insightBusiness.GetResult(monthEnding);

            var now = StringUtil.GetMonthEnding(monthEnding);
            var nowString = string.Format("{0:yyyy-MM-dd}", now);

            using (var dmClient = new HttpClient()) {
                var postObject = new {
                    data = result
                };

                var postData = JsonConvert.SerializeObject(postObject, new JsonSerializerSettings {
                    ContractResolver = new DefaultContractResolver {
                        NamingStrategy = new CamelCaseNamingStrategy()
                    }
                });

                dmClient.DefaultRequestHeaders.Add("X-Api-Key", dmApiKey);
                var response = await dmClient.PostAsync($"{dmApiUrl}/insight?monthEnding={nowString}", new StringContent(postData, Encoding.UTF8, "application/json"));

                return new OkObjectResult(new {
                    nowString,
                    response.StatusCode
                });
            }
        }
    }
}
