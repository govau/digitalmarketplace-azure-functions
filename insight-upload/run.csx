#r "Newtonsoft.Json"
#r "System"
#r "System.Data.SqlClient"
#r "Microsoft.Extensions.Configuration.Abstractions"
#r "Microsoft.Extensions.Configuration"
#r "Microsoft.Extensions.Configuration.EnvironmentVariables"
#r "Microsoft.Extensions.Configuration.FileExtensions"
#r "Microsoft.Extensions.Configuration.Json"


using System.Data.SqlClient;
using System.Net;
using System.Net.Http;
using System.Text;
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

    var dmApiUrl = Environment.GetEnvironmentVariable("dmApiUrl");
    var dmApiKey = Environment.GetEnvironmentVariable("dmApiKey");
    var insightsUrl = Environment.GetEnvironmentVariable("insightsUrl");

    var monthEnding = req.Query["monthEnding"];
    var now = DateTime.Now;
    now = new DateTime(now.Year, now.Month, 1);
    if (string.IsNullOrWhiteSpace(monthEnding) == false) {
        DateTime.TryParse(monthEnding, out now);
        now = new DateTime(now.Year, now.Month, 1);
    }
    var nowString = string.Format("{0:yyyy-MM-dd}", now);

    using (var azureClient = new HttpClient()) 
    using (var dmClient = new HttpClient()) {
        var insightData = await azureClient.GetStringAsync($"{insightsUrl}monthEnding={nowString}");

        var postObject = new {
            data = JsonConvert.DeserializeObject(insightData)
        };
        var postData = JsonConvert.SerializeObject(postObject);

        dmClient.DefaultRequestHeaders.Add("X-Api-Key", dmApiKey);
        var response = await dmClient.PostAsync($"{dmApiUrl}/insight?now={nowString}", new StringContent(postData, Encoding.UTF8, "application/json"));
        
        return  new OkObjectResult(new {
            nowString,
            response.StatusCode
        });
    }
}
