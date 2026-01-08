# NHS TAC Data Analysis - Session Summary

**Date:** 2026-01-03
**Branch:** claude/analyze-data-s1PxP
**Database:** 9.6 million records, 294 NHS organizations, 7 financial years (2017-18 to 2023-24)

---

## 🎯 What We Accomplished

### 1. **Metadata Analysis** ✅
- Analyzed TAC schema structure across 1,205 unique SubCodes
- Identified 770 stable subcodes (present in all 5 years)
- Mapped 32 distinct worksheets covering all NHS financial reporting
- Documented schema evolution (major restructuring in 2022-23)

**Outputs:**
- `Data/analysis/NHS_TAC_Analysis_Report.md`
- 6 visualizations showing provider distribution, schema evolution, TAC structure
- CSV exports: provider_analysis.csv, subcode_analysis.csv, summary_statistics.csv

### 2. **Consultancy Spending Analysis** ✅
- Identified all consultancy-related SubCodes through manual review
- Analyzed consultancy spending across all 294 organizations, 7 years
- Calculated consultancy as % of operating income (turnover)
- Sector comparison (Foundation Trusts vs NHS Trusts)

**Key Scripts:**
- `src/consultancy_comprehensive_analysis.py`

**Outputs:**
- `consultancy_by_org_year.csv` - Every org's spend by year
- `consultancy_with_turnover_pct.csv` - Spend as % of turnover
- `top_consultancy_spenders_total.csv` - Biggest spenders
- `sector_comparison.csv` - FT vs Trust analysis
- Comprehensive 4-panel visualization dashboard

### 3. **Outlier Detection** ✅
- Statistical analysis using z-scores to identify outliers (>2 std deviations)
- Detected organizations with unusually high/low consultancy spending
- Growth rate outliers (2017-18 to 2023-24)
- Sector-specific outlier analysis

**Script:**
- `src/outlier_analysis.py`

**Outputs:**
- `high_outliers_consultancy_pct.csv` - Top spenders as % of turnover
- `low_outliers_consultancy_pct.csv` - Most efficient (best practice candidates)
- `high_outliers_absolute_spend.csv` - Biggest absolute budgets
- `high_growth_outliers.csv` - Rapid increases
- `negative_growth_outliers.csv` - Rapid decreases
- `outliers_ft.csv` & `outliers_trust.csv` - Sector-specific
- `outlier_dashboard.png` - 5-panel visual dashboard

### 4. **Code Discovery Tools** ✅
- Created tools to identify IT and consultancy SubCodes
- Comprehensive code browser for manual review
- Helper scripts to explore top spending categories

**Scripts:**
- `src/discover_it_consultancy_working.py`
- `src/comprehensive_code_browser.py`
- `src/manual_code_identification_helper.py`
- `src/analyze_user_identified_codes.py`

### 5. **Activity Data Integration Framework** ✅
- Documented plan for integrating NHS activity data (beds, admissions, A&E, outpatients)
- Created template integration script
- Identified 9 key NHS data sources

**Documentation:**
- `ACTIVITY_DATA_PLAN.md`
- `src/integrate_activity_data.py` (template ready for data)

---

## 📊 Key Findings (Available in Generated Reports)

### Consultancy Spending:
- Complete year-on-year analysis for all organizations
- Consultancy as % of turnover calculated
- Sector comparisons show spending patterns
- Statistical outliers identified for investigation

### Data Quality:
- **Perfect data quality** - 0% null values across 9.6M records
- Strong provider participation (56% active across all years)
- Stable core schema (64% of subcodes consistent)

---

## 🔄 Current Status

### ✅ Completed:
1. Full database exploration and metadata analysis
2. Consultancy spending comprehensive analysis
3. Outlier detection and statistical analysis
4. Code discovery and verification tools
5. Activity data integration planning

### ⏳ Ready for Next Steps:
1. **IT Spending Analysis** - Awaiting manual code identification
2. **Activity Data Integration** - Framework ready, awaiting data download
3. **Normalized Metrics** - Will calculate per-bed, per-admission metrics once activity data integrated
4. **Best Practice Identification** - Can analyze low-spending outliers
5. **Correlation Studies** - Consultancy vs performance/outcomes

---

## 📁 Repository Structure

```
nhs-tru-tac/
├── Data/
│   ├── analysis/
│   │   ├── NHS_TAC_Analysis_Report.md
│   │   ├── visualizations/ (6 PNG files)
│   │   ├── database_analysis/ (from analyze_tac_database.py)
│   │   ├── consultancy_detailed/ (from consultancy_comprehensive_analysis.py)
│   │   ├── outlier_analysis/ (from outlier_analysis.py)
│   │   └── code_discovery/ (SubCode identification CSVs)
│   ├── canonical/
│   │   ├── tru_tac.duckdb (354 MB)
│   │   └── fact_tru_tac.parquet (13 MB)
│   ├── reference/ (NHS TAC illustrative files)
│   └── mappings/ (dimension tables)
├── src/
│   ├── analyze_tac_metadata.py
│   ├── create_visualizations.py
│   ├── analyze_tac_database.py
│   ├── consultancy_comprehensive_analysis.py ⭐
│   ├── outlier_analysis.py ⭐
│   ├── discover_it_consultancy_working.py
│   ├── comprehensive_code_browser.py
│   ├── integrate_activity_data.py
│   └── [15+ other analysis scripts]
├── ACTIVITY_DATA_PLAN.md
└── README.md (if exists)
```

---

## 🚀 Next Steps & Recommendations

### Immediate Priorities:

1. **Review Outlier Reports**
   - Open `Data/analysis/outlier_analysis/` files
   - Identify organizations for deeper investigation
   - Review `outlier_dashboard.png` for visual overview

2. **IT Code Identification** (Manual)
   - Review `Data/analysis/code_discovery/top_50_opex_for_manual_review.csv`
   - Identify IT-related SubCodes from operating expenses
   - Add to `src/analyze_user_identified_codes.py`
   - Run IT analysis

3. **Activity Data Integration**
   - Download bed data from: https://www.england.nhs.uk/statistics/statistical-work-areas/bed-availability-and-occupancy/
   - Download admissions data from: https://digital.nhs.uk/data-and-information/publications/statistical/hospital-admitted-patient-care-activity
   - Place files in `Data/activity/`
   - Update and run `src/integrate_activity_data.py`
   - Re-run outlier analysis with per-bed normalization

### Analytical Deep Dives:

4. **Best Practice Analysis**
   - Study low-spending outliers (efficient organizations)
   - Identify common characteristics
   - Document best practices

5. **Trend Analysis**
   - Time series analysis of consultancy trends
   - Correlation with NHS policy changes
   - Seasonal patterns

6. **Benchmarking**
   - Create peer groups (similar size/type)
   - Compare performance within groups
   - Generate benchmarking reports

7. **Correlation Studies**
   - Consultancy spend vs deficit/surplus
   - Consultancy vs CQC ratings (if available)
   - Regional patterns

---

## 📌 Important Notes

### Data Sources:
- **Financial Data:** NHS TAC (Treatment Advisory Commitment) returns
- **Years Covered:** 2017-18 to 2023-24
- **Organizations:** 294 (194 FTs, 100 Trusts)
- **Records:** 9,574,521

### Consultancy Codes Identified:
- Search terms: 'consult', 'advisory', 'professional'
- Verified through manual review
- Multiple SubCodes across different worksheets

### IT Codes Status:
- **Not yet fully identified** - automated searches didn't find clear IT codes
- Requires manual review of operating expense categories
- Framework ready for analysis once codes identified

### Data Quality:
- **Excellent** - No missing values in key fields
- Consistent organization naming
- Complete time series for most providers

---

## 🛠️ Scripts Quick Reference

### Run These Anytime:

```powershell
# Full database overview
python src\analyze_tac_database.py

# Comprehensive consultancy analysis
python src\consultancy_comprehensive_analysis.py

# Outlier detection
python src\outlier_analysis.py

# Code discovery (to find IT/consultancy codes)
python src\comprehensive_code_browser.py
```

### Run After Adding Data:

```powershell
# After identifying IT codes, edit and run:
python src\analyze_user_identified_codes.py

# After downloading activity data:
python src\integrate_activity_data.py
```

---

## 💡 Key Insights for Further Investigation

### Questions the Data Can Answer:

1. **Which organizations are consultancy outliers?**
   ✅ Available now in outlier_analysis outputs

2. **What is consultancy spending as % of turnover?**
   ✅ Available now in consultancy_with_turnover_pct.csv

3. **How has consultancy spending changed over time?**
   ✅ Available now - see trend visualizations

4. **Are larger trusts more efficient with consultancy?**
   ⏳ Pending activity data integration

5. **Is there a correlation between consultancy spend and financial performance?**
   ⏳ Can analyze once we define performance metrics

6. **What are regional patterns?**
   ⏳ Needs geographic/ICS mapping

### Potential Red Flags to Investigate:
- Organizations with consultancy >X% of turnover
- Rapid increases in consultancy spend
- Trusts with declining quality but increasing consultancy
- High consultancy spend with poor financial performance

---

## 📞 Support & Documentation

### All Analysis Outputs Located In:
```
C:\Users\laure\OneDrive\Documents\BevanBriefing\nhs-tru-tac\Data\analysis\
```

### Git Repository:
- **Branch:** claude/analyze-data-s1PxP
- **Status:** All work committed and pushed
- **Ready for:** Pull request or continued analysis

### Key Documents:
1. `Data/analysis/NHS_TAC_Analysis_Report.md` - Comprehensive metadata report
2. `ACTIVITY_DATA_PLAN.md` - Plan for activity data integration
3. This file - Session summary

---

## ✅ Ready to Proceed

All tools are in place for:
- ✅ Consultancy analysis (complete)
- ✅ Outlier detection (complete)
- ⏳ IT analysis (framework ready, awaiting code identification)
- ⏳ Activity-normalized metrics (framework ready, awaiting data)
- ⏳ Advanced analytics (benchmarking, correlations, best practices)

**The foundation is built. The analysis can now expand in any direction needed!**

---

*Generated: 2026-01-03*
*Branch: claude/analyze-data-s1PxP*
*Database: tru_tac.duckdb (9.6M records)*
