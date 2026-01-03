import pandas as pd

path = r"Data/reference/1920-nhs-provider-tac-ilustrative-file.xlsx"
xls = pd.ExcelFile(path, engine="openpyxl")
print("\n".join(xls.sheet_names))
