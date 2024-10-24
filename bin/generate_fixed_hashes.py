import os
import csv
import hashlib
import shutil

from pathlib import Path

def hash_file(filepath):
    """Generate SHA-256 hash of a file's contents."""
    sha256 = hashlib.sha256()
    try:
        with open(filepath, 'rb') as f:
            while chunk := f.read(8192):
                sha256.update(chunk)
        return sha256.hexdigest()
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return None

def generate_hash_csv(input_directory, output_directory, output_csv):
    """Generate a CSV file with the filename and its content hash, and save files in a new directory."""
    # Ensure the output directory exists
    os.makedirs(output_directory, exist_ok=True)
    
    with open(output_csv, mode='w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['old_resource', 'status', 'resource','notes'])

        # Walk through the directory and process each file
        for root, _, files in os.walk(input_directory):
            for file in files:
                filepath = os.path.join(root, file)
                file_hash = hash_file(filepath)
                if file_hash:
                    # Create a new file name based on the hash
                    new_filename = f"{file_hash}"
                    new_filepath = os.path.join(output_directory, new_filename)
                    
                    # Copy the file to the new directory under the new name
                    try:
                        shutil.copy2(filepath, new_filepath)
                        print(f"Copied {file} to {new_filename}")
                    except Exception as e:
                        print(f"Error copying {file}: {e}")
                        continue

                    # Write to CSV
                    writer.writerow([Path(file).stem,'301', file_hash,'Migration from fixed file'])
                else:
                    print(f"Skipping {file} due to read error")

# Define the directories and the output CSV file
input_directory = './fixed_changed'
output_directory = './hashed_files'
output_csv_file = 'file_hashes.csv'

# Generate the CSV with file hashes and copy files to new directory
generate_hash_csv(input_directory, output_directory, output_csv_file)
print(f"Hashes and new file paths written to {output_csv_file}")

