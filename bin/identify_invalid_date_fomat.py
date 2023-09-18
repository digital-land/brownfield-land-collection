import csv
import re

# Define a regular expression pattern for DD/MM/YYYY date format
date_pattern = r'\d{2}/\d{2}/\d{4}'

# Open the CSV file for reading
with open('collection/endpoint.csv', 'r', newline='') as csvfile:
    # Create a CSV reader
    csv_reader = csv.reader(csvfile)

    # Iterate through the rows in the CSV file
    for row in csv_reader:
        # Join the row data into a single string
        row_data = ','.join(row)

        # Check for the date pattern in the row data
        match = re.search(date_pattern, row_data)
        if match:
            print("Match found:", row_data)
