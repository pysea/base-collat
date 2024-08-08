import numpy as np
import pandas as pd
import configparser
import os, sys, glob
import shutil
from datetime import datetime, date
from collections import defaultdict, OrderedDict
import re
import zipfile
import operator

# Own modules
import utils
import copy_source_files as source

# parameters
Config = configparser.ConfigParser()

Config.read("config.ini")

source_path = Config['PATHS']['source_path']
hf_path = Config['PATHS']['hf_path']
pbi_path = Config['PATHS']['pbi_path']
results_path = Config['PATHS']['results_path']
eepe_path = Config['PATHS']['eepe_path']

# Get the current date and time
now = datetime.now()
# Format the date and time as a string
date_time_str = now.strftime("%Y-%m-%d_%H-%M-%S")

def create_HF_df(folder_path, type_file):

    # List to hold the data frames
    df_list = []
    lst_controls = []

    for ix, path in enumerate(folder_path):
        print(f'--\n{path}\n--')
        with zipfile.ZipFile(path, 'r') as zip_ref:
            # Get list of all archived file names from zip
            for member in zip_ref.namelist():
                # Check if the file is a CSV and contains "trade" in its name
                if type_file in member and member.endswith('.csv'):
                    print(f'--\nmember : {member}\n--')
                    with zip_ref.open(member) as file:
                        # Read the CSV file into a DataFrame
                        print('Reading file.\n--')
                        match = re.search(r'(\d{4})-(\d{2})-(\d{2})', member)
                        date = datetime.strptime(match.group(0), '%Y-%m-%d')
                        print(file)
                        df = utils.import_csv_force_format(metric='HF', source_file_type='', filename=member, full_path_file=file)
                        #df = pd.read_csv(file, encoding='latin1', sep=';')

                        df['date_prod'] = date
                        df['date_id'] = ix
                        df['name_file'] = member

                        # Append the DataFrame to the list
                        df_list.append(df)

    # Concatenate all the DataFrames in the list into a single DataFrame
    combined_df = pd.concat(df_list, ignore_index=True, axis=0)

    # Optionally, save the combined DataFrame to a new CSV file
    #combined_df.to_csv(f'combined_{type_file}.csv', index=False, encoding='latin1', sep=';')

    return combined_df

def get_2_most_recent_zip_files(path):

    files_path = os.path.join(path, '*')

    #lst_files = sorted(glob.iglob(files_path), key=os.path.getctime, reverse=False)
    files = glob.glob(files_path)
    files.sort(key=os.path.getmtime, reverse=True)

    return files[:2]



def control_1(df, results_path, lst_res, level_control_1_HF, operator_control_1_HF):

    lst_cols = [
        'ST_RateSensiDV01_1M_USD', 'ST_CreditSensiOAS01_1M_USD', "ST_RateSensiDV01_MtmEur", 'ST_CreditSensiOAS01_MtmEur'
    ]
    date_file_j = df.loc[0, 'date_file']
    nb_row = len(df)

    dic = {}
    for col in lst_cols:
        nb_null = len(df[df[col].isnull()])
        control_missing = nb_null/nb_row
        print(f'control_missing : {control_missing}')
        print(f'operator_control_1_HF {operator_control_1_HF}, level_control_1_HF : {level_control_1_HF}, control_missing {control_missing}')
        test_control_1 = utils.operand[operator_control_1_HF](control_missing, level_control_1_HF)
        print(f'test_control_1 : {test_control_1}')
        if test_control_1 == False:
            statut = 'KO'
            utils.export_control_csv(results_path, df[df[col].isnull()], foldername='control_1_2_HF_Missing_Sensi_DV01_OAS01',
                                     filename=f'control_1_Base_Collat_{col}')
        else:
            statut = 'OK'


        lst_res.append([
            date_time_str,
            f'control_1_Missing Sensi DV01 and OAS1',
            f'Exhaustivity: Le nombre de deals sans sensibilité champs DV01 et OAS1 ne dépasse pas 2% du total',
            col,
            '',
            control_missing,
            statut
        ])
        lst_res.append([
            date_time_str,
            f'control_2_Missing Sensi ISIN DV01 and OAS1',
            f"Le nombre ISIN sans sensibilité (champs DV01 et OAS1 vides ) doit être 0",
            col,
            date_file_j,
            str(nb_null),
            statut
        ])


    print(f' control_1_HF : OK.')
    return lst_res

def control_2(df, results_path, lst_res, level_control_2_HF, operator_control_2_HF):

    lst_cols = [
        'ST_RateSensiDV01_1M_USD', 'ST_CreditSensiOAS01_1M_USD', "ST_RateSensiDV01_MtmEur", 'ST_CreditSensiOAS01_MtmEur'
    ]
    date_file_j = df.loc[0, 'date_file']
    nb_row = len(df)

    dic = {}
    for col in lst_cols:
        nb_null = len(df[df[col].isnull()])
        print(f'nb_null : {nb_null}')
        test_control = utils.operand[operator_control_2_HF](nb_null, level_control_2_HF)
        print(f'test_control : {test_control}')
        if test_control == False:
            statut = 'KO'
            utils.export_control_csv(results_path, df[df[col].isnull()], foldername='control_2_HF_Missing_Sensi_DV01_OAS01',
                                     filename=f'control_2_Base_Collat_{col}')
        else:
            statut = 'OK'


        lst_res.append([
            date_time_str,
            f'control_2_Missing Sensi ISIN DV01 and OAS1',
            f"Le nombre ISIN sans sensibilité (champs DV01 et OAS1 vides ) doit être 0",
            col,
            date_file_j,
            str(nb_null),
            statut
        ])


    print(f' control_2 : OK.')
    return lst_res

def control_3(df, results_path, lst_res, level_control_3_HF, operator_control_3_HF):

    lst_cols = [
        'ST_StsDownRateImpact', 'ST_StsDownCreditImpact', "ST_StsUpRateImpact", 'ST_StsUpCreditImpact'
    ]
    date_file_j = df.loc[0, 'date_file']
    nb_row = len(df)

    dic = {}
    for col in lst_cols:
        nb_null = len(df[df[col].isnull()])
        print(f'nb_null : {nb_null}')
        control_missing = nb_null / nb_row
        print(f'control_missing : {control_missing}')
        print(
            f'operator_control_3_HF {operator_control_3_HF}, level_control_3_HF : {level_control_3_HF}, control_missing {control_missing}')
        test_control = utils.operand[operator_control_3_HF](control_missing, level_control_3_HF)
        print(f'test_control : {test_control}')
        if test_control == False:
            statut = 'KO'
            utils.export_control_csv(results_path, df[df[col].isnull()], foldername='control_3_HF_Missing_Impact',
                                     filename=f'control_3_Base_Collat_{col}')
        else:
            statut = 'OK'


        lst_res.append([
            date_time_str,
            f'control_3_Missing Impact',
            f"Le nombre de deals sans Stress Tests ne dépasse pas 3% du total.",
            col,
            date_file_j,
            control_missing,
            statut
        ])
        lst_res.append([
            date_time_str,
            f'control_3_Missing Impact',
            f"Le nombre de deals sans Stress Tests ne dépasse pas 3% du total.",
            col,
            date_file_j,
            nb_null,
            statut
        ])



    print(f' control_2 : OK.')
    return lst_res

def control_4(df, results_path, lst_res, level_control_4_HF, operator_control_4_HF):

    date_file_j = df[0].loc[0, 'date_file']

    df_j, df_j1 = df[0], df[1]
    print(f'col df_j : {list(df_j.columns)}, df_j1 : {list(df_j1.columns)}')
    col = [col for col in df_j.columns if 'isEurex' in col][0]
    df_j = df_j[~df_j[col]==True]
    df_j1 = df_j1[~df_j1[col]==True]
    nb_row = len(df_j)
    print(f'nb_row : {nb_row}')
    print(df_j.columns)
    print(df_j1.columns)
    df_merge = pd.merge(df_j, df_j1, how='left', on='ï»¿REPO_UniqueId', suffixes=('_j', '_j1'))
    df_merge['test'] = df_merge['ST_StsCreditDownScenario_j'] == df_merge['ST_StsCreditDownScenario_j1']
    df_test = df_merge[df_merge['test']==False]
    df_test = df_test[['ï»¿REPO_UniqueId', 'ST_StsCreditDownScenario_j', 'ST_StsCreditDownScenario_j1', 'test']]
    nb = len(df_test)
    print(f'nb : {nb}')

    utils.export_control_csv(results_path, df_test, foldername='control_4_HC_HaircutCheck', filename=f'control_4_Base_Collat_')


    lst_res.append([
        date_time_str,
        f'control_4_HC_HaircutCheck',
        f"Vérifier entre J-1 et J pour chaque ISIN stock constant, si la valeur ST_StsCreditDownScenario est la même",
        '',
        date_file_j,
        nb,
        ""
    ])

    print(f' control_4 : OK.')
    return lst_res

def control_5(df, results_path, lst_res, level_control_5_HF, operator_control_5_HF):

    date_file_j = df.loc[0, 'date_file']

    nb_row = len(df)
    print(f'nb_row : {nb_row}')
    df['CPTY_Type2'] = df['CPTY_Type']
    df['CPTY_Type2'] = df['CPTY_Type2'].replace({'INTRAGROUP': 'INTERNAL', 'AFFILIATES': 'INTERNAL'})
    df['test'] = df.apply(lambda x: x['CPTY_Type2'] == x['CPTY_Internal/External_GROUP'] == x['CPTY_Internal/External_GFS'], axis = 1)

    df_test = df[df['test']==False]
    df_test = df_test[['ï»¿REPO_UniqueId', 'CPTY_Type', 'CPTY_Type2', 'CPTY_Internal/External_GROUP', 'CPTY_Internal/External_GFS', 'test']]

    nb = len(df_test)
    print(f'nb : {nb}')
    control_missing = 1 - (nb / nb_row)
    print(f'control_missing : {control_missing}')
    test_control = utils.operand[operator_control_5_HF](control_missing, level_control_5_HF)
    if test_control == False:
        statut = 'KO'
        utils.export_control_csv(results_path, df_test, foldername='control_5_HC_CPTY_Type', filename=f'control_5_Base_Collat_')
    else:
        statut = 'OK'

    lst_res.append([
        date_time_str,
        f'control_5_HC_Coherence_CPTY_Type',
        f"Vérifier entre J-1 et J pour chaque ISIN stock constant, si la valeur ST_StsCreditDownScenario est la même",
        '',
        date_file_j,
        control_missing,
        statut
    ])

    lst_res.append([
        date_time_str,
        f'control_5_HC_Coherence_CPTY_Type',
        f"Vérifier entre J-1 et J pour chaque ISIN stock constant, si la valeur ST_StsCreditDownScenario est la même",
        '',
        '',
        nb,
        statut
    ])

    print(f' control_5 : OK.')
    return lst_res

def control_7(path_csv, results_path, lst_res, level_control_7_HF, operator_control_7_HF, file_path=None):

    print(f'path_csv : {path_csv}')
    file_size = os.path.getsize(path_csv)
    size = file_size / 1024 ** 2
    print(f'file_size : {file_size}, size : {size} MB')
    test_control = utils.operand[operator_control_7_HF](size, level_control_7_HF)
    print(f'operator_control_7_HF : {operator_control_7_HF}, level_control_7_HF : {level_control_7_HF}, size : {size}')
    print(f'test_control : {test_control}')

    if test_control == False:statut = 'KO'
    else:statut = 'OK'

    lst_res.append([
        date_time_str,
        f'control_7_File size',
        f"La taille du fichier doit être > 10Mo",
        '',
        '',
        f'size : {round(size, 2)} MB',
        statut
    ])

    return lst_res

def control_8(path_csv_1, path_csv_2, results_path, lst_res, level_control_8_HF, operator_control_8_HF, file_path=None):

    print(f'path_csv_1 : {path_csv_1}')
    file_size1 = os.path.getsize(path_csv_1)
    size1 = file_size1 / 1024 ** 2
    print(f'file_size1 : {file_size1}, size : {size1} MB')

    print(f'path_csv_2 : {path_csv_2}')
    file_size2 = os.path.getsize(path_csv_2)
    size2 = file_size2 / 1024 ** 2
    print(f'file_size2 : {file_size2}, size : {size2} MB')

    variation = abs(size1/size2-1)
    print(f'variation : {variation}')

    test_control = utils.operand[operator_control_8_HF](variation, level_control_8_HF)
    print(f'operator_control_8_HF : {operator_control_8_HF}, level_control_8_HF : {level_control_8_HF}, variation : {variation}')
    print(f'test_control : {test_control}')

    if test_control == False:statut = 'KO'
    else:statut = 'OK'

    lst_res.append([
        date_time_str,
        f'control_7_File size',
        f"La taille du fichier doit être > 10Mo",
        '',
        '',
        f'size_J : {round(size1, 2)} MB, size_J1 : {round(size2, 2)} MB, variation : {variation} MB',
        statut
    ])

    return lst_res

def control_10(df, results_path, lst_res, level_control_10_HF, operator_control_10_HF):

    date_file_j = df[0].loc[0, 'date_file']

    df_j, df_j1 = df[0], df[1]
    nb_row = len(df_j)
    print(f'nb_row : {nb_row}')
    nb_row2 = len(df_j1)
    print(f'nb_row2 : {nb_row2}')

    control_missing = nb_row - nb_row2
    print(f'control_missing : {control_missing}')

    lst_j = df_j['ï»¿REPO_UniqueId'].tolist()
    lst_j1 = df_j1['ï»¿REPO_UniqueId'].tolist()

    deal_in = [id for id in lst_j if id not in lst_j1]
    deal_out = [id for id in lst_j1 if id not in lst_j]
    print(f'deal_in : {len(deal_in)}')
    print(f'deal_out : {len(deal_out)}')

    df_in = df_j[df_j['ï»¿REPO_UniqueId'].isin(deal_in)]
    df_out = df_j1[df_j1['ï»¿REPO_UniqueId'].isin(deal_out)]

    utils.export_control_csv(results_path, df_in, foldername='control_10_HC_Stock', filename=f'control_10_Base_Collat_New_Deal_')
    utils.export_control_csv(results_path, df_out, foldername='control_10_HC_Stock', filename=f'control_10_Base_Collat_Out_Deal_')

    lst_res.append([
        date_time_str,
        f'control_10_HC_Stock',
        f"Contrôle la variation du nombre de deals entre J et J-1",
        '',
        date_file_j,
        control_missing,
        f'New deal : {len(deal_in)}, Out Deal : {len(deal_out)}'
    ])


    print(f' control_10 : OK.')
    return lst_res

def control_11(df, results_path, lst_res, level_control_11_HF, operator_control_11_HF):

    print(f'control_11 : Start.')

    date_file_j = df[0].loc[0, 'date_file']

    df_j, df_j1 = df[0], df[1]

    df_j = df_j[['ï»¿REPO_UniqueId', 'CPTY_IsHedgeFund']]
    df_j1 = df_j1[['ï»¿REPO_UniqueId', 'CPTY_IsHedgeFund']]

    df_merge = pd.merge(df_j, df_j1, how='left', on='ï»¿REPO_UniqueId', suffixes=('_j', '_j1'))
    df_merge['test'] = df_merge['CPTY_IsHedgeFund_j'] == df_merge['CPTY_IsHedgeFund_j1']
    df_test = df_merge[
        (df_merge['test']==False) & \
        (
            (df_merge['CPTY_IsHedgeFund_j']=='Y') |
            (df_merge['CPTY_IsHedgeFund_j1']=='Y')
        )
    ]
    nb = len(df_test)
    print(f'operator_control_11_HF {operator_control_11_HF}, level_control_11_HF : {level_control_11_HF}, control_missing {nb}')
    test_control = utils.operand[operator_control_11_HF](nb, level_control_11_HF)
    print(f'test_control : {test_control}')
    if test_control == False:
        statut = 'KO'
        utils.export_control_csv(results_path, df_test, foldername='control_12_HC_HaircutCheck',
                                 filename=f'control_12_Base_Collat_')
    else:
        statut = 'OK'


    lst_res.append([
        date_time_str,
        f'control_11_HC_HaircutCheck',
        f"LVariation en nombre de contrepartie flagguées CPTY_isHedgeFund = Y ne doit pas varier entre 2 jours ",
        '',
        date_file_j,
        nb,
        statut
    ])

    print(f' control_11 : OK.')

    return lst_res

def control_12(df, results_path, lst_res, level_control_12_HF, operator_control_12_HF):

    lst_cols = [
        'ST_StsDownRateImpact', 'ST_StsDownCreditImpact', "ST_StsUpRateImpact", 'ST_StsUpCreditImpact'
    ]

    date_file_j = df.loc[0, 'date_file']

    nb_row = len(df)
    print(f'nb_row : {nb_row}')
    df_test = df[df['HC_HaircutCheck'].isin(['KO'])]
    nb = len(df_test)
    print(f'nb : {nb}')
    control_missing = nb / nb_row
    print(f'control_missing : {control_missing}')
    print(
        f'operator_control_12_HF {operator_control_12_HF}, level_control_12_HF : {level_control_12_HF}, control_missing {control_missing}')
    test_control = utils.operand[operator_control_12_HF](control_missing, level_control_12_HF)
    print(f'test_control : {test_control}')
    if test_control == False:
        statut = 'KO'
        utils.export_control_csv(results_path, df_test, foldername='control_12_HC_HaircutCheck',
                                 filename=f'control_12_Base_Collat_')
    else:
        statut = 'OK'


    lst_res.append([
        date_time_str,
        f'control_12_HC_HaircutCheck',
        f"Le nombre de deals avec HC_HaircutCheck = KO est inférieur à 1%",
        '',
        date_file_j,
        control_missing,
        statut
    ])
    lst_res.append([
        date_time_str,
        f'control_12_HC_HaircutCheck',
        f"Le nombre de deals avec HC_HaircutCheck = KO est inférieur à 1%",
        '',
        date_file_j,
        nb,
        statut
    ])

    print(f' control_12 : OK.')
    return lst_res

def control_13(df, results_path, lst_res, level_control_13_HF, operator_control_13_HF):

    lst_cols = [
        'ST_StsDownRateImpact', 'ST_StsDownCreditImpact', "ST_StsUpRateImpact", 'ST_StsUpCreditImpact'
    ]

    date_file_j = df.loc[0, 'date_file']

    nb_row = len(df)
    print(f'nb_row : {nb_row}')
    lst = ['ERROR', 'NOT_FOUND']
    df_test = df[df['HC_HaircutCheck'].str.contains('|'.join(lst))]
    nb = len(df_test)
    print(f'nb : {nb}')
    control_missing = nb / nb_row
    print(f'control_missing : {control_missing}')
    print(
        f'operator_control_12_HF {operator_control_13_HF}, level_control_12_HF : {level_control_13_HF}, control_missing {control_missing}')
    test_control = utils.operand[operator_control_13_HF](control_missing, level_control_13_HF)
    print(f'test_control : {test_control}')
    if test_control == False:
        statut = 'KO'
        utils.export_control_csv(results_path, df_test, foldername='control_13_HC_HaircutCheck',
                                 filename=f'control_13_Base_Collat_')
    else:
        statut = 'OK'


    lst_res.append([
        date_time_str,
        f'control_13_HC_HaircutCheck',
        f"Le nombre de deals avec HC_HaircutCheck contenant ERROR ou NOT FOUND est inférieur à 1%",
        '',
        date_file_j,
        control_missing,
        statut
    ])
    lst_res.append([
        date_time_str,
        f'control_13_HC_HaircutCheck',
        f"Le nombre de deals avec HC_HaircutCheck contenant ERROR ou NOT FOUND est inférieur à 1%",
        '',
        date_file_j,
        nb,
        statut
    ])

    print(f' control_13 : OK.')
    return lst_res

def control_14(df, results_path, lst_res, level_control_14_HF, operator_control_14_HF):

    date_file_j = df.loc[0, 'date_file']

    nb_row = len(df)
    print(f'nb_row : {nb_row}')
    lst = ['IGNORE', 'ignore']
    df_test = df[df['HC_HaircutCheck'].str.contains('|'.join(lst))]
    nb = len(df_test)
    print(f'nb : {nb}')
    control_missing = nb / nb_row
    print(f'control_missing : {control_missing}')
    print(
        f'operator_control_14_HF {operator_control_14_HF}, level_control_14_HF : {level_control_14_HF}, control_missing {control_missing}')
    test_control = utils.operand[operator_control_14_HF](control_missing, level_control_14_HF)
    print(f'test_control : {test_control}')
    if test_control == False:
        statut = 'KO'
        utils.export_control_csv(results_path, df_test, foldername='control_14_HC_HaircutCheck', filename=f'control_14_Base_Collat_')
    else:
        statut = 'OK'


    lst_res.append([
        date_time_str,
        f'control_14_HC_HaircutCheck',
        f"Le nombre de deals avec HC_HaircutCheck contenant IGNORE est inférieur à 80%",
        '',
        date_file_j,
        control_missing,
        statut
    ])
    lst_res.append([
        date_time_str,
        f'control_14_HC_HaircutCheck',
        f"Le nombre de deals avec HC_HaircutCheck contenant IGNORE est inférieur à 80%",
        '',
        date_file_j,
        nb,
        statut
    ])

    print(f' control_14 : OK.')
    return lst_res

def control_15(df, results_path, lst_res, level_control_15_HF, operator_control_15_HF):

    date_file_j = df.loc[0, 'date_file']

    nb_row = len(df)
    print(f'nb_row : {nb_row}')
    lst = ['IGNORE collat ratings missing']
    df_test = df[df['HC_HaircutCheck'].str.contains('|'.join(lst))]
    #df_test = df[df['CPTY_worstRating'].isnull()]
    nb = len(df_test)
    print(f'nb : {nb}')
    control_missing = nb / nb_row
    print(f'control_missing : {control_missing}')
    print(
        f'operator_control_15_HF {operator_control_15_HF}, level_control_15_HF : {level_control_15_HF}, control_missing {control_missing}')
    test_control = utils.operand[operator_control_15_HF](control_missing, level_control_15_HF)
    print(f'test_control : {test_control}')
    if test_control == False:
        statut = 'KO'
        utils.export_control_csv(results_path, df_test, foldername='control_15_HC_HaircutCheck', filename=f'control_15_Base_Collat_')
    else:
        statut = 'OK'


    lst_res.append([
        date_time_str,
        f'control_15_HC_HaircutCheck',
        f"Le nombre de deals avec HC_HaircutCheck contenant IGNORE et dont le rating est absent est inférieur à 1%",
        '',
        date_file_j,
        control_missing,
        statut
    ])
    lst_res.append([
        date_time_str,
        f'control_15_HC_HaircutCheck',
        f"Le nombre de deals avec HC_HaircutCheck contenant IGNORE et dont le rating est absent est inférieur à 1%",
        '',
        date_file_j,
        nb,
        statut
    ])

    print(f' control_15 : OK.')
    return lst_res

def control_16(df, results_path, lst_res, level_control_16_HF, operator_control_16_HF):

    date_file_j = df.loc[0, 'date_file']

    nb_row = len(df)
    print(f'nb_row : {nb_row}')
    df_test = df[df['HC_HaircutCheck'].isin(['OK','ok'])]
    nb = len(df_test)
    print(f'nb : {nb}')
    control_missing = nb / nb_row
    print(f'control_missing : {control_missing}')
    print(
        f'operator_control_16_HF {operator_control_16_HF}, level_control_16_HF : {level_control_16_HF}, control_missing {control_missing}')
    test_control = utils.operand[operator_control_16_HF](control_missing, level_control_16_HF)
    print(f'test_control : {test_control}')
    if test_control == False:
        statut = 'KO'
        utils.export_control_csv(results_path, df_test, foldername='control_16_HC_HaircutCheck', filename=f'control_16_Base_Collat_')
    else:
        statut = 'OK'


    lst_res.append([
        date_time_str,
        f'control_16_HC_HaircutCheck',
        f"Le nombre de deals avec HC_HaircutCheck =OK est supérieur à 20%",
        '',
        date_file_j,
        control_missing,
        statut
    ])
    lst_res.append([
        date_time_str,
        f'control_16_HC_HaircutCheck',
        f"Le nombre de deals avec HC_HaircutCheck =OK est supérieur à 20%",
        '',
        date_file_j,
        nb,
        statut
    ])

    print(f' control_16 : OK.')
    return lst_res

def control_17(df, results_path, lst_res, level_control_17_HF, operator_control_17_HF, source_path):

    print(f'control_17 : Start')
    print(f'source_path : {source_path}')

    #date_file_j = df.loc[0, 'date_file']

    source_path = source_path.replace('Ã©', 'é')
    lst_files = [file for file in glob.glob1(source_path, '*.zip')]
    lst_files = [file for file in lst_files if file == f'{df}.zip']
    print(lst_files)
    nb = len(lst_files)
    print(f'nb : {nb}')
    print(
        f'operator_control_17_HF {operator_control_17_HF}, level_control_17_HF : {level_control_17_HF}, nb {nb}')
    test_control = utils.operand[operator_control_17_HF](nb, level_control_17_HF)
    print(f'test_control : {test_control}')
    if test_control == False:statut = 'KO'
    else:statut = 'OK'


    lst_res.append([
        date_time_str,
        f'control_17_Cohérence fichier',
        f"Le fichier (archive zip contenant le fichier .csv) doit être présent dans le dossier",
        '',
        '',
        nb,
        statut
    ])

    print(f' control_17 : OK.')
    return lst_res

def control_18(df, results_path, lst_res, level_control_18_HF, operator_control_18_HF, source_path):

    print(f'control_18 : Start')

    #date_file_j = df.loc[0, 'date_file']

    nb = utils.is_one_working_day_diff(df)

    test_control = utils.operand[operator_control_18_HF](nb, level_control_18_HF)
    print(f'test_control : {test_control}')
    if test_control == False:statut = 'KO'
    else:statut = 'OK'


    lst_res.append([
        date_time_str,
        f'control_18_Cohérence fichier',
        f"Le fichier doit être généré en J+1 (en jours ouvrables, par rapport à la date dans le nom du fichier)",
        '',
        '',
        f'nb difference working days : {nb}',
        statut
    ])

    print(f' control_18 : OK.')
    return lst_res

def control_19(df, results_path, lst_res, level_control_19_HF, operator_control_19_HF, source_path):

    print(f'control_19 : Start')

    date_file_j = df.loc[0, 'date_file']
    nb_row = len(df)
    df_hf = utils.hf_concat_csv_files(hf_path, '2024-07-19')
    print(f'def : {df_hf}')
    df['CPTY_TGL_CODE'] =  df['CPTY_TGL_CODE'].astype(object)
    df_hf['Mnemo'] =  df_hf['Mnemo'].astype(object)
    df_merge = pd.merge(df, df_hf, how='left', left_on='CPTY_TGL_CODE', right_on='Mnemo')
    dic_map = {'HF counterpart':'Y', 'HF Counterpart':'Y'}
    df_merge['Fund_category_2'] = df_merge['Fund category'].map(dic_map)
    df_merge['test'] = df_merge['Fund_category_2'] == df_merge['CPTY_IsHedgeFund']

    df_test = df_merge[
        ['Mnemo', 'CPTY_TGL_CODE', 'Fund category', 'Fund_category_2', 'CPTY_IsHedgeFund', 'test']][
        (df_merge['test']==False) & \
        (df_merge['CPTY_IsHedgeFund']=='Y') & \
        (~df_merge['Mnemo'].isnull())

    ]
    nb = len(df_test)
    print(f'nb : {nb}')
    if nb == 0:control_missing=1
    else:control_missing = 1 - (nb / nb_row)
    print(f'control_missing : {control_missing}')
    print(f'operator_control_19_HF {operator_control_19_HF}, level_control_19_HF : {level_control_19_HF}, control_missing {control_missing}')
    test_control = utils.operand[operator_control_19_HF](control_missing, level_control_19_HF)
    print(f'test_control : {test_control}')
    if test_control == False:
        statut = 'KO'
        utils.export_control_csv(results_path, df_test, foldername='control_19_HC_HaircutCheck', filename=f'control_19_Base_Collat_')
    else:
        statut = 'OK'

    lst_res.append([
        date_time_str,
        f'control_19_Cohérence fichier',
        f"Le fichier doit être généré en J+1 (en jours ouvrables, par rapport à la date dans le nom du fichier)",
        '',
        date_file_j,
        '',
        statut
    ])

    lst_res.append([
        date_time_str,
        f'control_19_Cohérence fichier',
        f"Le fichier doit être généré en J+1 (en jours ouvrables, par rapport à la date dans le nom du fichier)",
        '',
        date_file_j,
        f'{nb}',
        statut
    ])

    print(f' control_19 : OK.')
    return lst_res

def control_20(df, results_path, lst_res, level_control_20_HF, operator_control_20_HF, source_path):

    print(f'control_20 : Start')
    nb_row = len(df)

    date_file_j = df.loc[0, 'date_file']

    for file in glob.glob(f'{source_path}\\*'):
        if '.tgz' not in file:
            print(file)
            date_folder = file.split('\\')[-1]
            print(date_folder)
            if len(date_folder) == 8:
                year = date_folder[:4]
                month = date_folder[4:6]
                day = date_folder[6:]
                date_folder = f'{year}-{month}-{day}'
                if date_folder == date_file_j:
                    print(f'date_folder : {date_folder}, date_file_j : {date_file_j} ')
                    eepe_file = [f for f in glob.glob1(file, '*.csv') if 'SFT' in f][0]
                    print(f'eepe_file : {eepe_file}')
                    df_eepe = pd.read_csv(f'{file}\\{eepe_file}', sep=';', encoding='latin1', low_memory=False, skiprows=[0,1,2,3,4,5,6])

                    #df_eepe['PosB2C_IdCom_IdFo'] = df_eepe['PosB2C_IdCom_IdFo'].str.replace(r'[A-Za-z]', '', regex=True)
                    #df['REPO_TradeId'] = df['REPO_TradeId'].str.replace(r'[A-Za-z]', '', regex=True)

                    lst_id_eepe = set(df_eepe['PosB2C_IdCom_IdFo'].tolist())
                    nb_eepe = len(lst_id_eepe)

                    lst_id_df = set(df['REPO_TradeId'].tolist())

                    lst_in_df_in_eepe = [id for id in lst_id_df if id in lst_id_eepe]
                    lst_in_df_not_in_eepe = [id for id in lst_id_df if id not in lst_id_eepe]
                    lst_in_eepe_not_in_df = [id for id in lst_id_eepe if id not in lst_id_df]

                    df_in_df_in_eepe = df[df['REPO_TradeId'].isin(lst_in_df_in_eepe)]
                    df_in_df_not_in_eepe = df[df['REPO_TradeId'].isin(lst_in_df_not_in_eepe)]
                    df_in_eepe_not_in_df = df_eepe[df_eepe['PosB2C_IdCom_IdFo'].isin(lst_in_eepe_not_in_df)]

                    print(f'df_in_df_in_eepe : {len(df_in_df_in_eepe)}, df_in_df_not_in_eepe : {len(df_in_df_not_in_eepe)}, df_in_eepe_not_in_df : {len(df_in_eepe_not_in_df)} ')

                    control_missing = 1 - (nb_row - nb_eepe)
                    print(f'control_missing : {control_missing}')
                    print(f'operator_control_20_HF {operator_control_20_HF}, level_control_20_HF : {level_control_20_HF}, control_missing {control_missing}')
                    test_control = utils.operand[operator_control_20_HF](control_missing, level_control_20_HF)
                    print(f'test_control : {test_control}')
                    if test_control == False:
                        statut = 'KO'
                        utils.export_control_csv(results_path, df_in_df_in_eepe, foldername='control_20_HC_HaircutCheck', filename=f'control_20_Base_Collat_df_in_df_in_eepe_')
                        utils.export_control_csv(results_path, df_in_df_not_in_eepe, foldername='control_20_HC_HaircutCheck', filename=f'control_20_Base_Collat_df_in_df_not_in_eepe_')
                        utils.export_control_csv(results_path, df_in_eepe_not_in_df, foldername='control_20_HC_HaircutCheck', filename=f'control_20_Base_Collat_df_in_eepe_not_in_df')
                    else:
                        statut = 'OK'

                    lst_res.append([
                        date_time_str,
                        f'control_20_Cohérence fichier',
                        f"Contrôler mensuellement le stock entre le référentiel arrêté EEPE (BXAM)",
                        '',
                        date_file_j,
                        f'nb id in base collat file and in eepe : {len(df_in_df_in_eepe)}',
                        statut
                    ])

                    lst_res.append([
                        date_time_str,
                        f'control_20_Cohérence fichier',
                        f"Contrôler mensuellement le stock entre le référentiel arrêté EEPE (BXAM)",
                        '',
                        date_file_j,
                        f'nb id in base collat file and not in eepe : {len(df_in_df_not_in_eepe)}',
                        statut
                    ])

                    lst_res.append([
                        date_time_str,
                        f'control_20_Cohérence fichier',
                        f"Contrôler mensuellement le stock entre le référentiel arrêté EEPE (BXAM)",
                        '',
                        date_file_j,
                        f'nb id in eepe file and not in base collat file : {len(df_in_eepe_not_in_df)}',
                        statut
                    ])


            else:
                print('Folder EEPE has not been found')
                continue


    print(f' control_20 : OK.')
    return lst_res

def run_controls():
    df_controls = pd.read_excel(f'controls_list.xlsx')

    # Finds operand
    operator_control_1_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_1_Base_Collat'].values[0]
    operator_control_2_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_2_Base_Collat'].values[0]
    operator_control_3_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_3_Base_Collat'].values[0]
    operator_control_4_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_4_Base_Collat'].values[0]
    operator_control_5_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_4_Base_Collat'].values[0]
    #operator_control_6_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_6_HF'].values[0]
    operator_control_7_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_7_Base_Collat'].values[0]
    operator_control_8_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_8_Base_Collat'].values[0]
    operator_control_10_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_10_Base_Collat'].values[0]
    operator_control_11_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_11_Base_Collat'].values[0]
    #operator_control_9_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_9_HF'].values[0]
    operator_control_12_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_12_Base_Collat'].values[0]
    operator_control_13_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_13_Base_Collat'].values[0]
    operator_control_14_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_14_Base_Collat'].values[0]
    operator_control_15_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_15_Base_Collat'].values[0]
    operator_control_16_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_16_Base_Collat'].values[0]
    operator_control_17_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_17_Base_Collat'].values[0]
    operator_control_18_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_18_Base_Collat'].values[0]
    operator_control_19_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_19_Base_Collat'].values[0]
    operator_control_20_HF = df_controls['Operator'][df_controls['Python_function'] == 'control_20_Base_Collat'].values[0]

    # Finds levels
    level_control_1_HF = float(df_controls['Level'][df_controls['Python_function'] == 'control_1_Base_Collat'].values[0])
    level_control_2_HF = int(df_controls['Level'][df_controls['Python_function'] == 'control_2_Base_Collat'].values[0])
    level_control_3_HF = float(df_controls['Level'][df_controls['Python_function'] == 'control_3_Base_Collat'].values[0])
    level_control_4_HF = int(df_controls['Level'][df_controls['Python_function'] == 'control_4_Base_Collat'].values[0])
    level_control_5_HF = int(df_controls['Level'][df_controls['Python_function'] == 'control_4_Base_Collat'].values[0])
    #level_control_6_HF = float(df_controls['Level'][df_controls['Python_function'] == 'control_6_HF'].values[0])
    level_control_7_HF = int(df_controls['Level'][df_controls['Python_function'] == 'control_7_Base_Collat'].values[0])
    level_control_8_HF = float(df_controls['Level'][df_controls['Python_function'] == 'control_8_Base_Collat'].values[0])

    level_control_10_HF = df_controls['Level'][df_controls['Python_function'] == 'control_10_Base_Collat'].values[0]
    level_control_11_HF = df_controls['Level'][df_controls['Python_function'] == 'control_11_Base_Collat'].values[0]
    level_control_12_HF = float(df_controls['Level'][df_controls['Python_function'] == 'control_12_Base_Collat'].values[0])
    level_control_13_HF = float(df_controls['Level'][df_controls['Python_function'] == 'control_13_Base_Collat'].values[0])
    level_control_14_HF = float(df_controls['Level'][df_controls['Python_function'] == 'control_14_Base_Collat'].values[0])
    level_control_15_HF = float(df_controls['Level'][df_controls['Python_function'] == 'control_15_Base_Collat'].values[0])
    level_control_16_HF = float(df_controls['Level'][df_controls['Python_function'] == 'control_16_Base_Collat'].values[0])
    level_control_17_HF = int(df_controls['Level'][df_controls['Python_function'] == 'control_17_Base_Collat'].values[0])
    level_control_18_HF = int(df_controls['Level'][df_controls['Python_function'] == 'control_18_Base_Collat'].values[0])
    level_control_19_HF = int(df_controls['Level'][df_controls['Python_function'] == 'control_19_Base_Collat'].values[0])
    level_control_20_HF = int(df_controls['Level'][df_controls['Python_function'] == 'control_20_Base_Collat'].values[0])

    #path_csv = f'C:\\Users\\bmarchand\\OneDrive - Groupe BPCE\\prod-test\\CrmExtract_Summit_2024-07-19.csv'
    #path_csv2 = f'C:\\Users\\bmarchand\\OneDrive - Groupe BPCE\\prod-test\\CrmExtract_Summit_2024-07-18.csv'
    lst_files = source.get_files_sorted_by_date(pbi_path, 'csv')
    #lst_files = get_2_most_recent_zip_files(pbi_path)
    print(f'lst_files : {lst_files}')

    filename_j = lst_files[0].split('\\')[-1]
    print(f'filename_j : {filename_j}')

    dic_df = {}
    for ix, file in enumerate(lst_files):
        df = pd.read_csv(os.path.join(pbi_path, file), sep=';', encoding='latin1', low_memory=False)
        date_file = file.split('_')[-1][:-4]
        df['date_file'] = date_file
        dic_df[ix] = df

    print(dic_df)

    lst_res = []

    lst_res = control_1(dic_df[0], results_path, lst_res, level_control_1_HF, operator_control_1_HF)
    lst_res = control_2(dic_df[0], results_path, lst_res, level_control_2_HF, operator_control_2_HF)
    lst_res = control_3(dic_df[0], results_path, lst_res, level_control_3_HF, operator_control_3_HF)
    lst_res = control_4(dic_df, results_path, lst_res, level_control_4_HF, operator_control_4_HF)
    lst_res = control_5(dic_df[0], results_path, lst_res, level_control_5_HF, operator_control_5_HF)
    lst_res = control_7(os.path.join(pbi_path, lst_files[0]), results_path, lst_res, level_control_7_HF, operator_control_7_HF)
    lst_res = control_8(os.path.join(pbi_path, lst_files[0]), os.path.join(pbi_path, lst_files[1]), results_path, lst_res, level_control_8_HF, operator_control_8_HF)
    lst_res = control_10(dic_df, results_path, lst_res, level_control_10_HF, operator_control_10_HF)
    lst_res = control_11(dic_df, results_path, lst_res, level_control_11_HF, operator_control_11_HF)
    lst_res = control_12(dic_df[0], results_path, lst_res, level_control_12_HF, operator_control_12_HF)
    lst_res = control_13(dic_df[0], results_path, lst_res, level_control_13_HF, operator_control_13_HF)
    lst_res = control_14(dic_df[0], results_path, lst_res, level_control_14_HF, operator_control_14_HF)
    lst_res = control_15(dic_df[0], results_path, lst_res, level_control_15_HF, operator_control_15_HF)
    lst_res = control_16(dic_df[0], results_path, lst_res, level_control_16_HF, operator_control_16_HF)
    lst_res = control_17(filename_j, results_path, lst_res, level_control_17_HF, operator_control_17_HF, source_path)
    lst_res = control_18(filename_j, results_path, lst_res, level_control_18_HF, operator_control_18_HF, source_path)
    lst_res = control_19(dic_df[0], results_path, lst_res, level_control_19_HF, operator_control_19_HF, hf_path)
    lst_res = control_20(dic_df[0], results_path, lst_res, level_control_20_HF, operator_control_20_HF, eepe_path)

    df_result_final = pd.read_csv(os.path.join(results_path, f'results_prod_stress_test.csv'), encoding='latin1',
                                  sep=';')
    df_res = pd.DataFrame(
        lst_res,
        columns=[
            'Date_prod', 'Controlled Data', 'Control',
            'File Type', 'Date Prod', 'Value', 'Status']
    )

    lst_final = [df_result_final, df_res]
    df_res_export = pd.concat(lst_final, axis=0, ignore_index=True)
    df_res_export.to_csv(
        os.path.join(results_path, f'results_prod_stress_test.csv'),
        index=False,
        encoding='latin1',
        sep=';',
        decimal=','
    )

    print('Done')

##############################################################
if __name__=='__main__':
    run_controls()