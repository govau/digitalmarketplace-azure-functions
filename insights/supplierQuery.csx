#r "System.Data.SqlClient"

#load "query.csx"
#load "vwRptMarketplaceSupplier.csx"
#load "../NameCount.csx"

using System.Data.SqlClient;

internal class SupplierQuery : Query {

    private readonly string _query = @"
SELECT [Supplier ABN]
      ,[Supplier Name]
      ,[Supplier SME Status (MP)]
      ,[Supplier SME Status]
      ,[Supplier Creation Date]
      ,[Supplier Creation Date (Financial Year)]
      ,[Supplier Creation Date (Month Ending)]
      ,[Supplier Creation Date Latest Month Flag]
      ,[Supplier Status]
      ,[Agile delivery and Governance]
      ,[Change and Transformation]
      ,[Content and Publishing]
      ,[Cyber security]
      ,[Data science]
      ,[Emerging technologies]
      ,[Marketing, Communications and Engagement]
      ,[Software engineering and Development]
      ,[Strategy and Policy]
      ,[Support and Operations]
      ,[Training, Learning and Development]
      ,[User research and Design]
      ,[Digital sourcing and ICT procurement]
      ,[ICT risk management and audit activities]
      ,[ICT systems integration]
  FROM [Data].[VW_RPT_Marketplace_Supplier]
    ";
    private readonly DateTime _now;

    public SupplierQuery(DateTime now, string connectionString) : base(connectionString) {
        _now = now;
    }

    public async Task<List<VwRptMarketplaceSupplier>> GetDataAsync() {
        return await base.ExecuteQueryAsync<VwRptMarketplaceSupplier>(_query, (reader) => (
            new VwRptMarketplaceSupplier {
                SupplierABN = GetFieldValueOrNull<string>(reader, 0),
                SupplierName = GetFieldValueOrNull<string>(reader, 1),
                SupplierSMEStatusMP = GetFieldValueOrNull<string>(reader, 2),
                SupplierSMEStatus = GetFieldValueOrNull<string>(reader, 3),
                SupplierCreationDate = GetFieldValueOrNull<DateTime>(reader, 4),
                SupplierCreationDateFinancialYear = GetFieldValueOrNull<string>(reader, 5),
                SupplierCreationDateMonthEnding = GetFieldValueOrNull<DateTime>(reader, 6),
                SupplierCreationDateLatestMonthFlag = GetFieldValueOrNull<string>(reader, 7),
                SupplierStatus = GetFieldValueOrNull<string>(reader, 8),
                AgileDeliveryAndGovernance = GetFieldValueOrNull<int?>(reader, 9),
                ChangeAndTransformation = GetFieldValueOrNull<int?>(reader, 10),
                ContentAndPublishing = GetFieldValueOrNull<int?>(reader, 11),
                CyberSecurity = GetFieldValueOrNull<int?>(reader, 12),
                DataScience = GetFieldValueOrNull<int?>(reader, 13),
                EmergingTechnologies = GetFieldValueOrNull<int?>(reader, 14),
                MarketingCommunicationsAndEngagement = GetFieldValueOrNull<int?>(reader, 15),
                SoftwareEngineeringAndDevelopment = GetFieldValueOrNull<int?>(reader, 16),
                StrategyAndPolicy = GetFieldValueOrNull<int?>(reader, 17),
                SupportAndOperations = GetFieldValueOrNull<int?>(reader, 18),
                TrainingLearningAndDevelopment = GetFieldValueOrNull<int?>(reader, 19),
                UserResearchAndDesign = GetFieldValueOrNull<int?>(reader, 20),
                DigitalSourcingAndICTProcurement = GetFieldValueOrNull<int?>(reader, 21),
                ICTRiskManagementAndAuditActivities = GetFieldValueOrNull<int?>(reader, 22),
                ICTSystemsIntegration = GetFieldValueOrNull<int?>(reader, 23)
            }
        ));
    }

    public async Task<dynamic> GetAggregationsAsync() {
        var data = await GetDataAsync();

        var supplierCount = data
            .Where(d => d.SupplierCreationDate.Date <= _now.Date)
            .Count();

        var suppliersCreatedThisMonth = data
            .Where(d => 
                d.SupplierCreationDate.Year == _now.Year &&
                d.SupplierCreationDate.Month == _now.Month)
            .Count();

        return new {
            supplierCount,
            suppliersCreatedThisMonth
        };
    }
}