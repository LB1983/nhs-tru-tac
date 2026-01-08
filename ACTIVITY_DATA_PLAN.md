# NHS Activity Data Integration Plan

## Data Sources Provided

### NHS England Statistics:
1. **Bed Availability & Occupancy**: https://www.england.nhs.uk/statistics/statistical-work-areas/bed-availability-and-occupancy/
2. **A&E Activity**: https://www.england.nhs.uk/statistics/statistical-work-areas/ae-waiting-times-and-activity/
3. **Diagnostic Imaging**: https://www.england.nhs.uk/statistics/statistical-work-areas/diagnostic-imaging-dataset/
4. **NHS Staff Survey**: https://www.england.nhs.uk/statistics/statistical-work-areas/nhs-staff-survey-in-england/

### NHS Digital:
5. **A&E Activity (detailed)**: https://digital.nhs.uk/data-and-information/publications/statistical/hospital-accident--emergency-activity
6. **Admitted Patient Care**: https://digital.nhs.uk/data-and-information/publications/statistical/hospital-admitted-patient-care-activity
7. **Critical Care**: https://digital.nhs.uk/data-and-information/publications/statistical/hospital-adult-critical-care-activity
8. **Outpatient Activity**: https://digital.nhs.uk/data-and-information/publications/statistical/hospital-outpatient-activity
9. **Maternity Statistics**: https://digital.nhs.uk/data-and-information/publications/statistical/nhs-maternity-statistics

## Priority Metrics for Integration

### Most Useful for Benchmarking:
1. **Beds** - Fundamental size measure
2. **Admitted Patients (Spells/Episodes)** - Core acute activity
3. **A&E Attendances** - Emergency workload
4. **Outpatient Attendances** - Planned care volume
5. **Staff Numbers (FTE)** - Workforce size

## Integration Approach

### Option 1: Manual Download (Quickest)
1. Download key datasets manually (CSV/Excel)
2. Place in `Data/activity/` folder
3. Run integration script to match organizations

### Option 2: Automated Download (More Complete)
1. Use Python to fetch data from URLs
2. Parse Excel/CSV files programmatically
3. Build comprehensive activity database

## Recommended Starting Point

**Start with Bed Data** - Most stable, available for all trusts, good proxy for size

### File to Download:
From: https://www.england.nhs.uk/statistics/statistical-work-areas/bed-availability-and-occupancy/

Look for files like:
- "Beds Open Overnight by Organisation"
- "Beds Time Series"
- Latest quarterly/annual data

### What We'll Create:

```
org_name | fy | beds | admissions | ae_attendances | op_attendances | consultancy_spend | consultancy_per_bed | consultancy_pct_turnover
---------|----|----- |------------|----------------|----------------|-------------------|---------------------|------------------------
Trust A  |2023| 500  | 50000      | 80000          | 150000         | £2.5M             | £5000               | 0.8%
```

## Next Steps

1. **Tell me which dataset to start with** (I recommend beds)
2. **Download the file** or **share the direct link** to a specific Excel/CSV
3. **I'll create an integration script** to:
   - Load activity data
   - Match to our TAC organizations
   - Calculate metrics per bed/per activity
   - Generate enriched analysis

## Questions to Clarify:

1. Do you want to download files manually or try automated scraping?
2. Which years are most important? (2017-18 to 2023-24 to match TAC data?)
3. Should we prioritize beds + admissions first, then add others later?

Let me know and I'll create the integration scripts!
