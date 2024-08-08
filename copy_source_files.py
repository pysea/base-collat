import configparser
import os, sys, glob
import shutil
from datetime import datetime, date
from collections import defaultdict, OrderedDict
import re
import zipfile
import shutil
import subprocess
import time

def find_most_recent_folders(directory, number_of_folders=4):
    # Define the regex pattern for folder names in the format YYYY-mm-dd
    date_pattern = re.compile(r'^\d{4}-\d{2}-\d{2}$')
    print(f'date_pattern : {date_pattern}')

    # List all items in the directory
    all_items = glob.glob1(directory, '*.csv')
    print(f'all_items : {all_items}')

    # Filter the items to keep only those that match the date pattern
    date_folders = [item for item in all_items if date_pattern.match(item)]

    # Convert folder names to datetime objects for comparison
    date_folders = [datetime.strptime(folder, '%Y-%m-%d') for folder in date_folders]

    # Sort the dates in descending order and get the most recent ones
    most_recent_dates = sorted(date_folders, reverse=True)[:number_of_folders]

    # Convert the most recent dates back to string format
    most_recent_folders = [os.path.join(directory, date.strftime('%Y-%m-%d')) for date in most_recent_dates]

    return most_recent_folders

def extract_file_date_from_filename(filename):
    date_pattern = r'(\d{4}-\d{2}-\d{2})'
    match = re.search(date_pattern, filename)
    if match:
        return datetime.strptime(match.group(0), '%Y-%m-%d')
    return None


def get_files_sorted_by_date(directory, extension):
    print(f'get_files_sorted_by_date : Start.')
    print(f'Listing files. Please wait...')
    files_with_dates = []

    for filename in glob.glob1(directory, f'*.{extension}'):
        print(f'filename : {filename}')
        if os.path.isfile(os.path.join(directory, filename)):
            file_date = extract_file_date_from_filename(filename)
            if file_date:
                files_with_dates.append((filename, file_date))

    sorted_files = sorted(files_with_dates, key=lambda x: x[1], reverse=True)
    return [file[0] for file in sorted_files if f'.{extension}' in file[0]]

def copy_zip_auto(source_path, pbi_path):
    source_path = source_path.replace('Ã©', 'é')
    print(f'source_path : {source_path}')
    sorted_files = get_files_sorted_by_date(source_path, 'zip')
    sorted_files = sorted_files[:2]
    print(f'sorted_files : {sorted_files}')

    # source.choose_your_files(source_path, pbi_path)

    for file in sorted_files:
        shutil.copyfile(os.path.join(source_path, file), os.path.join(pbi_path, file))

    for file in os.listdir(pbi_path):
        print(file)
        ziploc = "C:/Program Files/7-Zip/7z.exe"  # location where 7zip is installed
        cmd = [ziploc, 'e', os.path.join(pbi_path, file), '-o' + pbi_path, '-r']
        sp = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        time.sleep(2)

    for file in glob.glob1(pbi_path, '*.zip'):
        os.remove(os.path.join(pbi_path, file))

    print('File copied')


def choose_your_files(source_path, dest_path):

    source_path = source_path.replace('Ã©', 'é')

    file1 = input('Copy filename 1 : ' )
    file2 = input('Copy filename 2 : ' )

    shutil.copy(os.path.join(source_path, file1), os.path.join(dest_path, file1))
    shutil.copy(os.path.join(source_path, file2), os.path.join(dest_path, file2))

    for file in os.listdir(dest_path):
        print(file)
        ziploc = "C:/Program Files/7-Zip/7z.exe"  # location where 7zip is installed
        cmd = [ziploc, 'e', os.path.join(dest_path, file), '-o' + dest_path, '-r']
        sp = subprocess.Popen(cmd, stderr=subprocess.STDOUT, stdout=subprocess.PIPE)
        time.sleep(2)

    for file in glob.glob1(dest_path, '*.zip'):
        os.remove(os.path.join(dest_path, file))

    return file1, file2

# Case 1 : output folder is empty.
def copy_files_no_existing_files(source_path, output_path):
    with os.scandir(output_path) as it:
        if any(it):
            print('The output direction is not empty')
            print('Deleting...')
            for file in glob.glob1(output_path, '*.zip'):
                print(file)
                os.remove(os.path.join(output_path, file))
            print('Deleting : done.')
            print('Copying all files.')

            copy_zip_auto(source_path, output_path)

        else:
            print('Output folder is empty. Copying all files')
            print('Copying all files.')

            copy_zip_auto(source_path, output_path)



    print('function copy_files_no_existing_files : done.')
