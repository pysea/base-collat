import operator
import os, glob, sys
import pandas as pd
from datetime import datetime, date
import configparser
import re
import zipfile


# parameters
Config = configparser.ConfigParser()

Config.read("config.ini")

path_data_dict = Config['PATHS']['data_dict']


# Get the current date and time
now = datetime.now()
# Format the date and time as a string
date_time_str = now.strftime("%Y-%m-%d_%H-%M-%S")




# This dictionary is used with the user input file.
# It aims at giving control on the type of control
operand = {
    '<': operator.lt,
    '<=': operator.le,
    '=': operator.eq,
    '!=': operator.ne,
    '>': operator.gt,
    '>=': operator.ge,
}

def export_control_csv(results_path, df, foldername, filename):

    results_path = os.path.join(results_path, foldername)
    filename = filename + date_time_str + '.csv'

    print('test folder :')
    print(os.path.isfile(results_path))
    print(results_path)

    if os.path.isdir(results_path):
        print("Folder is already created")
    else:
        print("Doesn't exists. Creates folder")
        os.makedirs(os.path.join(results_path))

    df.to_csv(os.path.join(results_path, filename), index=False, encoding='latin1', sep=';', decimal=',')
    print(f'File : {filename} exported.')


def import_csv_force_format(metric, source_file_type, filename, full_path_file):

    print(f'import_csv_force_format : filename = {filename}')
    df_dict = pd.read_excel(path_data_dict)
    lst_file_type = set(df_dict['file_type'][df_dict['metric']==metric].tolist())
    file_type = [type for type in lst_file_type if type in filename][0]
    df_filter = df_dict[df_dict['metric']==metric]
    df_filter = df_dict[df_dict['file_type']==file_type]
    dic_types = dict(zip(df_filter['col_name_init'], df_filter['type']))
    df = pd.read_csv(full_path_file, sep=';', encoding='latin1')
    for col, typ in dic_types.items():
        print(f'col : {col}, typ: {typ}')
        if typ == 'float64' or typ == 'int':
            df[col] = pd.to_numeric(df[col], errors='coerce')
        elif typ == 'datetime':
            pd.to_datetime(df[col], errors='coerce')
        else:
            df[col] = df[col].astype(typ)

    return df


def is_one_working_day_diff(filename):
    """
    Check if the date in the filename is exactly one working day before the current date.

    Args:
        filename (str): The name of the CSV file in the format 'prefix_YYYY-mm-dd.csv'

    Returns:
        bool: True if the date difference is exactly one working day, False otherwise.
    """
    # Extract the date string from the filename
    try:
        date_str = filename.split('_')[-1].replace('.csv', '')
    except ValueError:
        # Handle cases where the date format is incorrect
        print("Invalid date format in filename.")
        return False

    # Get the current date
    current_date = date.today()
    print(f'current_date : {current_date}')

    file_date = datetime.strptime(date_str, '%Y-%m-%d').date()
    print(f'file_date : {file_date}')

    # Calculate the previous working day from the current date
    previous_working_day = (pd.date_range(end=current_date, periods=2, freq='B')[-2]).date()
    print(f'previous_working_day : {previous_working_day}')

    # Check if the file date is exactly one working day before the current date
    difference = (current_date - file_date).days
    print(f'difference : {difference} working days')

    #is_previous_working_day = file_date == previous_working_day
    #print(f'is_previous_working_day : {is_previous_working_day}')

    return difference


def hf_concat_csv_files(folder_path, target_date):
    """
    List all CSV files in ZIP files within a folder that contain the specified date and 'HF' in the filename,
    and concatenate them into a single DataFrame.

    Args:
        folder_path (str): Path to the folder containing ZIP files.
        target_date (str): Target date in the format 'YYYY-mm-dd'.

    Returns:
        DataFrame: A concatenated DataFrame of all matching CSV files.
    """
    date_pattern = re.compile(r'\d{4}-\d{2}-\d{2}')
    all_data = []

    for root, _, files in os.walk(folder_path):
        for file in files:
            if file.endswith('.zip'):
                zip_path = os.path.join(root, file)
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    for zip_info in zip_ref.infolist():
                        if zip_info.filename.endswith('.csv'):
                            csv_filename = os.path.basename(zip_info.filename)
                            if 'HF' in csv_filename and 'All' in csv_filename and date_pattern.search(csv_filename):
                                print(f'csv_filename : {csv_filename}')
                                file_date = date_pattern.search(csv_filename).group()
                                try:
                                    # Check if the date matches the target date
                                    if datetime.strptime(file_date, '%Y-%m-%d').date() == datetime.strptime(target_date,
                                                                                                            '%Y-%m-%d').date():
                                        print(f'zip_info.filename : {zip_info.filename}')
                                        with zip_ref.open(zip_info.filename) as csv_file:
                                            df = pd.read_csv(csv_file, sep=';', encoding='latin1', low_memory=False)
                                            print(df)

                                except ValueError:
                                    # Handle cases where the date format is incorrect
                                    continue

    return df


def transform_date(date_str):
    """
    Transforms a date from YYYYmmdd to YYYY-mm-dd format.

    Args:
    date_str (str): The date string in YYYYmmdd format.

    Returns:
    str: The date string in YYYY-mm-dd format.
    """
    if len(date_str) == 8:
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:]
    else:
        print('Not correct date format')

    return f"{year}-{month}-{day}"



