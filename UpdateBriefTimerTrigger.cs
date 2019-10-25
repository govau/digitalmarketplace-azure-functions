using System;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Configuration;
using Dta.Marketplace.Azure.Functions.Query;

namespace Dta.Marketplace.Azure.Functions {
    public static class UpdateBriefTimerTrigger {
        [FunctionName("UpdateBriefTimerTrigger")]
        public static async Task Run(
            [TimerTrigger("0 0 20 * * *")]TimerInfo timer,
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
                var response = await dmClient.GetStringAsync($"{dmApiUrl}/reports/brief/published");

                var briefQuery = new BriefQuery(DateTime.Now, connectionString);
                log.LogInformation($"rows updated: {await briefQuery.UpdateImpMarketplaceBrief(response)}");
            }
        }
    }
}
