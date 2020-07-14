using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Dta.Marketplace.Azure.Functions.Query;

namespace Dta.Marketplace.Azure.Functions {
    public static class UpdateBriefResponseTimerTrigger {
        [FunctionName("UpdateBriefResponseTimerTrigger")]
        public static async Task Run(
            [TimerTrigger("0 10 20 * * *")]TimerInfo timer,
            ILogger log, ExecutionContext context) {
            var config = new ConfigurationBuilder()
                .SetBasePath(context.FunctionAppDirectory)
                .AddJsonFile("local.settings.json", optional: true, reloadOnChange: true)
                .AddEnvironmentVariables()
                .Build();

            var connectionString = config.GetConnectionString("DevProcurementConnectionString");
            var dmApiUrl = Environment.GetEnvironmentVariable("dmApiUrl");
            var dmApiKey = Environment.GetEnvironmentVariable("dmApiKey");

            using (var dmClient = new HttpClient()) {
                dmClient.DefaultRequestHeaders.Add("X-Api-Key", dmApiKey);
                var response = await dmClient.GetStringAsync($"{dmApiUrl}/reports/brief_response/submitted");

                var briefResponseQuery = new BriefResponseQuery(DateTime.Now, connectionString);
                log.LogInformation($"rows updated: {await briefResponseQuery.UpdateImpMarketplaceBriefResponse(response)}");
            }
        }
    }
}
