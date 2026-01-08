# NHS Activity Data Download Guide

## 📥 Where to Save Files

Save all downloaded files to: **`Data/activity/`**

Create the folder if it doesn't exist:
```powershell
mkdir Data\activity
```

---

## 🛏️ STEP 1: Download Bed Data (PRIORITY)

### Go to:
https://www.england.nhs.uk/statistics/statistical-work-areas/bed-availability-and-occupancy/

### What to download:
1. Scroll down to find **"Bed Availability and Occupancy Data – Overnight"**
2. Look for the **most recent quarterly publication** (e.g., "Q2 2023-24")
3. Download the **Excel file** (usually called something like "Beds-Timeseries-[date].xlsx")

### Alternative - Time Series:
- Look for "Time series" or "Annual" file that covers 2017-18 to 2023-24
- This will have all years in one file

### Save as:
`Data/activity/beds.xlsx` or `Data/activity/beds.csv`

---

## 🚑 STEP 2: Download A&E Activity (OPTIONAL but useful)

### Go to:
https://digital.nhs.uk/data-and-information/publications/statistical/hospital-accident--emergency-activity

### What to download:
1. Find the **latest annual publication**
2. Download the **provider-level data** (look for "Provider" or "Trust level" file)
3. Should cover attendances by organization

### Save as:
`Data/activity/ae_attendances.xlsx`

---

## 🏥 STEP 3: Download Admissions Data (OPTIONAL but useful)

### Go to:
https://digital.nhs.uk/data-and-information/publications/statistical/hospital-admitted-patient-care-activity

### What to download:
1. Find the **latest annual publication**
2. Look for **provider-level** admissions/spells data
3. Download the Excel file

### Save as:
`Data/activity/admissions.xlsx`

---

## 📋 Expected File Structure

After downloading, you should have:

```
Data/
└── activity/
    ├── beds.xlsx (or beds.csv)
    ├── ae_attendances.xlsx (optional)
    └── admissions.xlsx (optional)
```

---

## ▶️ Next Step

Once you've downloaded the files, tell me:
1. **Which files you downloaded**
2. **What the column names are** (open in Excel and tell me the headers)

I'll then update the integration script to automatically:
- Load your files
- Match organization names
- Calculate consultancy per bed
- Re-run outlier analysis with normalized metrics

---

## 💡 Tips

### If you can't find exact files:
- Look for "Provider level" or "Trust level" data
- Quarterly data is fine - we can aggregate it
- CSV format is actually easier than Excel
- Time series files (all years in one) are best

### File name doesn't matter:
- Just save as beds.xlsx, ae_attendances.xlsx, etc.
- The script will adapt to the column structure

### Just start with beds:
- Beds alone will unlock powerful analysis
- You can add other activity data later

---

## 🆘 If You Get Stuck

If the websites are confusing, just:
1. Download ANY bed-related file you can find
2. Tell me what you downloaded
3. I'll adapt the script to work with it

The goal is to get:
- **Organization name** (or ODS code)
- **Number of beds** (or "available beds")
- **Year/Quarter**

Even partial data is useful!

---

Ready to download? Start with beds and let me know when you have it! 🎯
