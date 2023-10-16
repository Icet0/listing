import csv
import pandas as pd

def detect_separator(file_path):
    # List of potential separators to check
    separators = [',', ';', '\t']
    
    # Try different encodings
    encodings = ['utf-8', 'latin-1']

    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as file:
                # Read the first line of the file
                first_line = file.readline()

                # Use the csv sniffer to check if the separator works
                sniffer = csv.Sniffer()
                if sniffer.sniff(first_line).delimiter in separators:
                    return sniffer.sniff(first_line).delimiter
        except UnicodeDecodeError:
            # Continue to the next encoding if decoding fails
            continue
    
    # If no separator is found, return None
    return None



def read_csv_with_autodetect(file_path):
    encodings = ['utf-8', 'latin-1']
    separator = detect_separator(file_path)
    
    if separator:
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, sep=separator, encoding=encoding)
                return df
            except UnicodeDecodeError:
                # Continue to the next encoding if decoding fails
                continue
        print("Unable to read the CSV with any encoding.")
        return None
    else:
        print("Unable to detect the separator.")
        return None


