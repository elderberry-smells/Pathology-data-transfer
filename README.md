# Pathology-data-transfer
small bit of code used to convert ratings in a row-wise format to a column format.  The script looks into a target directory for the raw data files (csv format) and matches it to the POB file.  The POB is then uploaded with the raw data by converting the raw data to a column based format instead of row, then merges the 2 files with pandas.
