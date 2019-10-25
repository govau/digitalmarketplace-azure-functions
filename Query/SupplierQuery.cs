using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Dta.Marketplace.Azure.Functions.Model;

namespace Dta.Marketplace.Azure.Functions.Query {
    internal class SupplierQuery : BaseQuery {

        private readonly string _vwRptMarketplaceSupplierQuery = @"
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

        private readonly string _vwRptMarketplaceSupplierCategoryQuery = @"
SELECT [Supplier ABN]
      ,[Supplier Name]
      ,[Supplier Status]
      ,[Supplier Category Status]
      ,[Supplier Category]
  FROM [Data].[VW_RPT_Marketplace_Supplier_Category]
    ";

        private readonly string _updateImpMarketplaceSupplier = @"
            UPDATE [zImport].[IMP_Marketplace_Supplier]
            SET [Marketplace_Supplier_json] = @json
        ";

        private readonly DateTime _now;

        public SupplierQuery(DateTime now, string connectionString) : base(connectionString) {
            _now = now;
        }


        public async Task<List<VwRptMarketplaceSupplierCategory>> GetVwRptMarketplaceSupplierCategoryDataAsync() {
            return await base.ExecuteReaderAsync<VwRptMarketplaceSupplierCategory>(_vwRptMarketplaceSupplierCategoryQuery, (reader) => (
                new VwRptMarketplaceSupplierCategory {
                    SupplierABN = GetFieldValueOrNull<string>(reader, 0),
                    SupplierName = GetFieldValueOrNull<string>(reader, 1),
                    SupplierStatus = GetFieldValueOrNull<string>(reader, 2),
                    SupplierCategoryStatus = GetFieldValueOrNull<string>(reader, 3),
                    SupplierCategory = GetFieldValueOrNull<string>(reader, 4)
                }
            ));
        }

        public async Task<List<VwRptMarketplaceSupplier>> GetVwRptMarketplaceSupplierDataAsync() {
            return await base.ExecuteReaderAsync<VwRptMarketplaceSupplier>(_vwRptMarketplaceSupplierQuery, (reader) => (
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

        public async Task<int> UpdateImpMarketplaceSupplier(string json) {
            return await base.ExecuteNonQueryAsync(c => {
                var command = new SqlCommand(_updateImpMarketplaceSupplier, c);
                command.Parameters.AddWithValue("@json", json);
                return command;
            });
        }

        public async Task<dynamic> GetAggregationsAsync() {
            var vwRptMarketplaceSupplierData = await GetVwRptMarketplaceSupplierDataAsync();

            var supplierCount = vwRptMarketplaceSupplierData
                .Where(d => d.SupplierCreationDate.Date <= _now.Date)
                .Count();

            var suppliersCreatedThisMonth = vwRptMarketplaceSupplierData
                .Where(d =>
                    d.SupplierCreationDate.Year == _now.Year &&
                    d.SupplierCreationDate.Month == _now.Month)
                .Count();

            var vwRptMarketplaceSupplierCategoryData = await GetVwRptMarketplaceSupplierCategoryDataAsync();

            var numberOfSuppliersPerCategory = vwRptMarketplaceSupplierCategoryData
                .GroupBy(d => d.SupplierCategory,
                    (key, d) => new NameCount {
                        Name = key,
                        Count = d.Count()
                    }
                )
                .OrderBy(d => d.Name);

            return new {
                numberOfSuppliersPerCategory,
                supplierCount,
                suppliersCreatedThisMonth
            };
        }
    }
}