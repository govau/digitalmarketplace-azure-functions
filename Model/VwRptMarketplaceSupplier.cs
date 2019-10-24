using System;

namespace Dta.Marketplace.Azure.Functions.Model {
    internal class VwRptMarketplaceSupplier {
        public string SupplierABN { get; set; }
        public string SupplierName { get; set; }
        public string SupplierSMEStatusMP { get; set; }
        public string SupplierSMEStatus { get; set; }
        public DateTime SupplierCreationDate { get; set; }
        public string SupplierCreationDateFinancialYear { get; set; }
        public DateTime SupplierCreationDateMonthEnding { get; set; }
        public string SupplierCreationDateLatestMonthFlag { get; set; }
        public string SupplierStatus { get; set; }
        public int? AgileDeliveryAndGovernance { get; set; }
        public int? ChangeAndTransformation { get; set; }
        public int? ContentAndPublishing { get; set; }
        public int? CyberSecurity { get; set; }
        public int? DataScience { get; set; }
        public int? EmergingTechnologies { get; set; }
        public int? MarketingCommunicationsAndEngagement { get; set; }
        public int? SoftwareEngineeringAndDevelopment { get; set; }
        public int? StrategyAndPolicy { get; set; }
        public int? SupportAndOperations { get; set; }
        public int? TrainingLearningAndDevelopment { get; set; }
        public int? UserResearchAndDesign { get; set; }
        public int? DigitalSourcingAndICTProcurement { get; set; }
        public int? ICTRiskManagementAndAuditActivities { get; set; }
        public int? ICTSystemsIntegration { get; set; }
    }
}