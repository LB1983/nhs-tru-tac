# NHS TAC Data Analysis Report

**Analysis Date:** 2026-01-03
**Dataset:** NHS Trust and Foundation Trust Treatment Advisory Commitment (TAC) Metadata
**Financial Years Covered:** 2019-20 to 2023-24

---

## Executive Summary

This report presents a comprehensive analysis of NHS Treatment Advisory Commitment (TAC) data structure and metadata. The analysis covers **294 NHS providers** across **5 financial years** (2019-20 to 2023-24), examining the evolution of the TAC reporting schema, provider participation, and data structure.

### Key Findings

- **294 NHS Providers** tracked across the analysis period
  - 66% Foundation Trusts (FTs)
  - 34% NHS Trusts
  - 165 providers active continuously from 2017-18 to 2023-24

- **1,205 Unique SubCodes** across all years
  - 770 stable subcodes (present in all 5 years)
  - 71 volatile subcodes (present in only 1 year)
  - Average of ~990 subcodes per year

- **32 Distinct Worksheets** covering all aspects of NHS financial reporting
  - Top worksheets: Receivables (359 subcodes), Operating Expenses (349 subcodes), Cash Flow (308 subcodes)

- **813 TAC Lines** mapped across 9 TableIDs and 120 MainCodes

---

## 1. Provider Analysis

### 1.1 Provider Distribution

The dataset includes **294 unique NHS providers**:

| Sector | Count | Percentage |
|--------|-------|------------|
| Foundation Trust (FT) | 194 | 66.0% |
| NHS Trust | 100 | 34.0% |

**Visualization:** See `visualizations/01_provider_sector_distribution.png`

### 1.2 Provider Participation Over Time

- **First Year Seen:** 2017-18
- **Last Year Seen:** 2023-24
- **Continuously Active Providers (2017-18 to 2023-24):** 165 (56.1%)

This indicates strong longitudinal consistency, with over half of all providers maintaining continuous data submission throughout the entire period.

**Visualization:** See `visualizations/02_provider_activity_timeline.png`

### 1.3 Top Providers by Data Volume

The top 10 providers by total data rows all belong to the NHS Trust sector and have been continuously active from 2017-18 to 2023-24:

1. Avon and Wiltshire Mental Health Partnership NHS Trust (44,002 rows)
2. Barnet, Enfield And Haringey Mental Health NHS Trust (44,002 rows)
3. Barts Health NHS Trust (44,002 rows)
4. Buckinghamshire Healthcare NHS Trust (44,002 rows)
5. Cambridgeshire Community Services NHS Trust (44,002 rows)
6. Central London Community Healthcare NHS Trust (44,002 rows)
7. Coventry and Warwickshire Partnership NHS Trust (44,002 rows)
8. Dartford and Gravesham NHS Trust (44,002 rows)
9. Devon Partnership NHS Trust (44,002 rows)
10. East And North Hertfordshire NHS Trust (44,002 rows)

---

## 2. TAC Structure Analysis

### 2.1 Financial Years Coverage

The analysis covers **5 financial years:**
- 2019-20
- 2020-21
- 2021-22
- 2022-23
- 2023-24

### 2.2 Worksheet Distribution

The TAC framework includes **32 unique worksheets** across all years, covering:

- **Financial Statements:**
  - TAC02 SoCI (Statement of Comprehensive Income)
  - TAC03 SoFP (Statement of Financial Position)
  - TAC04 SOCIE (Statement of Changes in Equity)
  - TAC05 SoCF (Statement of Cash Flows)

- **Detailed Disclosures:**
  - TAC06-07: Operating Income
  - TAC08: Operating Expenses
  - TAC09: Staff Costs
  - TAC11: Finance & Other
  - TAC13-14: Intangibles & PPE
  - TAC18-21: Receivables, Payables, Borrowings
  - TAC24-27: PFI, Pensions, Financial Instruments
  - TAC28-29: Disclosures & Losses

#### Top 10 Worksheets by SubCode Count:

| Worksheet | SubCode Count |
|-----------|---------------|
| TAC18 Receivables | 359 |
| TAC08 Op Exp | 349 |
| TAC05 SoCF | 308 |
| TAC11 Finance & other | 283 |
| TAC14 PPE | 256 |
| TAC20 Payables | 241 |
| TAC21 Borrowings | 214 |
| TAC26 Pension | 210 |
| TAC29 Losses+SP | 209 |
| TAC13 Intangibles | 203 |

**Visualization:** See `visualizations/04_top_worksheets.png`

### 2.3 SubCode Analysis

**Total Unique SubCodes:** 1,205

#### SubCodes per Financial Year:

| Financial Year | Unique SubCodes |
|----------------|-----------------|
| 2019-20 | 983 |
| 2020-21 | 1,010 |
| 2021-22 | 1,018 |
| 2022-23 | 1,008 |
| 2023-24 | 947 |

The schema shows remarkable stability with approximately 1,000 subcodes per year, with a notable decrease in 2023-24 (947 subcodes).

**Visualization:** See `visualizations/03_subcode_evolution.png`

---

## 3. TAC Lines Analysis

### 3.1 Structure Overview

- **Total TAC Lines:** 813
- **Unique TableIDs:** 9
- **Unique MainCodes:** 120
- **Unique SubCodes in Lines:** 277

### 3.2 Distribution by TableID

| TableID | Line Count | Percentage |
|---------|------------|------------|
| Table 1 | 375 | 46.1% |
| Table 2 | 127 | 15.6% |
| Table 3 | 97 | 11.9% |
| Table 4 | 37 | 4.6% |
| Table 5 | 44 | 5.4% |
| Table 6 | 57 | 7.0% |
| Table 7 | 17 | 2.1% |
| Table 8 | 44 | 5.4% |
| Table 9 | 15 | 1.8% |

Table 1 dominates the distribution, accounting for nearly half of all TAC lines.

**Visualization:** See `visualizations/05_tac_lines_by_table.png`

### 3.3 Categorization Status

Currently, **0% of TAC lines have category_1 classification**, indicating an opportunity for enhanced categorization and metadata enrichment.

---

## 4. Schema Evolution Analysis

### 4.1 Year-over-Year Changes

| Financial Year | Total SubCodes | New SubCodes | Removed SubCodes |
|----------------|----------------|--------------|------------------|
| 2019-20 | 983 | 983 (baseline) | - |
| 2020-21 | 1,010 | 48 | 21 |
| 2021-22 | 1,018 | 15 | 7 |
| 2022-23 | 1,008 | 141 | 151 |
| 2023-24 | 947 | 21 | 82 |

**Key Observations:**

1. **Stable Growth (2019-21):** Incremental additions with minimal removals
2. **Major Restructuring (2022-23):** Significant changes with 141 new subcodes and 151 removals
3. **Consolidation (2023-24):** Net reduction of 61 subcodes, suggesting schema simplification

### 4.2 SubCode Stability

- **Stable SubCodes (present in all 5 years):** 770 (63.9%)
- **Volatile SubCodes (present in only 1 year):** 71 (5.9%)

This indicates a highly stable core schema with approximately 64% of subcodes remaining consistent across all years.

**Visualization:** See `visualizations/06_subcode_stability.png`

---

## 5. Data Quality & Completeness

### 5.1 Provider Data Completeness

- **Providers with complete time series (2017-18 to 2023-24):** 165 (56.1%)
- **Average data rows per provider:** Varies by provider size and type
- **Top provider data volume:** 44,002 rows

### 5.2 Schema Consistency

- **Core stable subcodes:** 770 (63.9% of all subcodes)
- **Schema volatility:** Moderate, with most changes occurring in 2022-23

---

## 6. Key Insights & Recommendations

### 6.1 Insights

1. **Strong Provider Participation:** Over half of providers maintain continuous reporting throughout the analysis period

2. **Stable Core Schema:** 64% of subcodes remain consistent across all years, providing a reliable foundation for longitudinal analysis

3. **2022-23 Schema Transition:** The most significant schema changes occurred in 2022-23, likely related to accounting standard updates (e.g., IFRS 16 implementation)

4. **Sector Balance:** Foundation Trusts dominate (66%), reflecting the progression of NHS organizations to FT status

5. **Comprehensive Coverage:** The TAC framework covers all major aspects of NHS financial reporting through 32 worksheets

### 6.2 Recommendations

1. **Categorization Enhancement:** Implement category_1 and category_2 classifications for TAC lines to enable thematic analysis

2. **Schema Documentation:** Document the rationale for 2022-23 schema changes to aid users in understanding structural breaks

3. **Volatility Management:** Investigate the 71 volatile subcodes (1-year only) to determine if they represent temporary reporting requirements or data quality issues

4. **Longitudinal Analysis Support:** Leverage the 770 stable subcodes for robust time-series analysis across all financial years

5. **Provider Segmentation:** Consider analyzing trends separately for continuously-active providers (165) vs. those with gaps in reporting

---

## 7. Technical Appendix

### 7.1 Data Sources

- **Provider Dimension:** `mappings/dim_provider.csv` (294 providers)
- **SubCode Mapping:** `mappings/dim_tac_subcodes_by_year.csv` (5,018 records)
- **TAC Lines:** `mappings/dim_tac_lines_seed.csv` (813 lines)

### 7.2 Analysis Scripts

All analysis was performed using Python 3 with the following key scripts:

- `src/analyze_tac_metadata.py` - Metadata analysis and statistics
- `src/create_visualizations.py` - Chart and graph generation
- `src/explore_illustrative_files.py` - TAC file structure exploration

### 7.3 Output Files

**Statistics:**
- `Data/analysis/summary_statistics.csv`
- `Data/analysis/provider_analysis.csv`
- `Data/analysis/subcode_analysis.csv`

**Visualizations:**
- `Data/analysis/visualizations/01_provider_sector_distribution.png`
- `Data/analysis/visualizations/02_provider_activity_timeline.png`
- `Data/analysis/visualizations/03_subcode_evolution.png`
- `Data/analysis/visualizations/04_top_worksheets.png`
- `Data/analysis/visualizations/05_tac_lines_by_table.png`
- `Data/analysis/visualizations/06_subcode_stability.png`

---

## 8. Glossary

- **TAC:** Treatment Advisory Commitment - NHS financial reporting framework
- **FT:** Foundation Trust
- **SubCode:** Specific line item code within TAC reporting
- **MainCode:** Primary categorization code
- **TableID:** Table identifier within TAC structure
- **SoCI:** Statement of Comprehensive Income
- **SoFP:** Statement of Financial Position
- **SOCIE:** Statement of Changes in Equity
- **SoCF:** Statement of Cash Flows
- **PPE:** Property, Plant & Equipment
- **PFI:** Private Finance Initiative

---

## Conclusion

The NHS TAC dataset demonstrates robust structure and strong provider participation across the 2019-24 period. With 770 stable subcodes forming a reliable core, the framework supports comprehensive financial analysis while accommodating necessary schema evolution. The 2022-23 restructuring represents a significant modernization event, likely tied to accounting standard updates. Future analyses should leverage the stable core for longitudinal insights while carefully managing schema transitions for accurate year-over-year comparisons.

---

**Report Generated By:** NHS TAC Analysis Pipeline
**Analysis Date:** 2026-01-03
**Repository:** nhs-tru-tac
