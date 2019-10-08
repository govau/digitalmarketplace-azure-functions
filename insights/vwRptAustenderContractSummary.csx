using System;

internal class VwRptAustenderContractSummary {
    public string ContractStartDateFinancialYear { get; set; }
    public DateTime ContractStartDateMonthEnding { get; set; }
    public string ContractStartDateLatestMonthFlag { get; set; }
    public string ContractStartDateLatestFinYearFlag { get; set; }
    public string ContractType { get; set; }
    public string ContractTypeLevel2 { get; set; }
    public string ContractValueGroup { get; set; }
    public string ContractSpendAgencyNameShort { get; set; }
    public string ContractSupplierName { get; set; }
    public string ContractSupplierMarketplaceName { get; set; }
    public string ContractSupplierSMELatestStatus { get; set; }
    public string ContractSupplierMarketplaceSMEStatus { get; set; }
    public string ContractSONID { get; set; }
    public string SONTitleLong { get; set; }
    public int TotalContracts { get; set; }
    public double TotalContractValueAmount { get; set; }
}