import csv
import pandas as pd
import os

convertfile = '2012 Firdale.csv'
projectbook = '2012 Firdale B1C01 POB.csv'

#we want to open both files and grab only the columns that are the same (unique identifiers only from convert file)

with open(projectbook, 'r') as res_file:
    res_reader = csv.DictReader(res_file)
    results_headers = res_reader.fieldnames

r = open(convertfile, 'rb')

# grabbing the header line from the convert file, and extracting the # of ratings for each entry
header_line = ['Entry Book Season', 'Entry Book Project']
header_length = 0
new_header = []

for line in r:
    bits = line.split(',')
    if bits[0] in header_line:
        headers = bits[:]
        ratings = []
        for i in headers:
            try:
                i = int(i)
                ratings.append(i)
            except:
                continue

        # make a list of all the headers in the file, and assign them to a variable
        header_length = 0
        for i in headers:
            try:
                i = int(i)
                break
            except:
                header_length += 1

        total_ratings = int(ratings[-1])
        new_header = bits[:header_length]
        new_header.append('Drag Copy#')
        new_header.append('Rating')

r.close()

convert_header = []
similar_header = []
for i in new_header:
    if i not in results_headers:
        continue
    else:
        convert_header.append(i)
        similar_header.append(i)
convert_header.append('Rating')


print 'convert_header: ',convert_header



# Make a temp file that has all the headers and ratings going down (row wise instead of column).  Once in a new file
# it can be read as a DictReader so we can extract the proper columns into a merge file with the Results file.

with open(projectbook, 'r') as f1:  # open results file
    reader = csv.DictReader(f1)
    res_headers = reader.fieldnames

    print 'results header: ',res_headers

# write a new file with headers, and the ratings going down instead of to the side

    with open('temp.csv', 'w') as csvfile:
        writer = csv.writer(csvfile, delimiter=',', lineterminator='\n')
        writer.writerow(new_header)

        n = open(convertfile, 'rb')

        for line in n:
            bits = line.split(',')

            if bits[0] == '' or bits[0] in header_line:
                continue

            else:

                dragcopy = 1
                rating_column = (header_length)
                while dragcopy < total_ratings + 1:
                    # write the sample info into the new spreadheet
                    new_transfer = bits[:header_length]

                    # write in the drag copy number (rating #) into the next column and add 1 to the count, this needs to be looped for the same amount of times as there are ratings
                    new_transfer.append(dragcopy)
                    dragcopy += 1

                    # add the rating to the next column for 'Rating'.  This needs to be done for the total_ratings amount
                    new_transfer.append(bits[rating_column])
                    rating_column += 1
                    # write the line in the while statement
                    writer.writerow(new_transfer)
        n.close()

#  open the convertfile and the temp file.  Write the proper columns, and only transfer the temp file columns
#  that are present in convert file

f = pd.read_csv('temp.csv')
new_f = f[convert_header]
new_f.to_csv('merge.csv', index=False)

# merge the 2 tables together with the pandas commands (left join on the Results file)
projectbook_df = pd.read_csv(projectbook, keep_default_na=False, na_values=[""])

merge_df = pd.read_csv('merge.csv', keep_default_na=False, na_values=[""])

merged_left = pd.merge(left=projectbook_df, right=merge_df, how ='left',
                       left_on=similar_header,
                       right_on=similar_header)

# write the merged file to a csv

merged_left.to_csv('example.csv', index=False)

os.remove('temp.csv')
os.remove('merge.csv')