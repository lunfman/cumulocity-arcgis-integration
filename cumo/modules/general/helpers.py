import csv
from io import StringIO
import os
from typing import List
import pandas as pd
from dagster import MetadataValue, OpExecutionContext, get_dagster_logger


logger = get_dagster_logger()


def convert_to_stream(data:list[dict]) -> StringIO:
    buffer = StringIO()
    writer = csv.DictWriter(buffer, fieldnames=data[0].keys())
    writer.writeheader()  # Write column headers
    writer.writerows(data) 
    buffer.seek(0)
    return buffer


def write_to_csv_file(file_name:str, data:list[dict]) -> None:
    is_file = os.path.exists(file_name)
    with open(file_name, mode="a", newline="") as file:
        writer = csv.DictWriter(file, data[0].keys())
        if not is_file:
            writer.writeheader()  # Write column headers
        writer.writerows(data)  

def read_csvs_from_local_pd(file_names:list[str], pk:dict) -> list[pd.DataFrame]:
    files = []
    for file_name in file_names:
        file_name = file_name.format(**pk)
        logger.info(file_name)
        try:
            files.append(pd.read_csv(file_name))
        except Exception as e:
            logger.log.info(e)
            continue      
    return files

def read_csvs_from_minio(file_names:list[str],bucket:str, context:OpExecutionContext, pk:dict) -> list[pd.DataFrame]:
    files = []
    for file_name in file_names:
        try:
            file_name = file_name.format(**pk)
            logger.info(file_name)
            res = context.resources.minio.get_obj(bucket, file_name)
            files.append(pd.read_csv(res))
        except Exception as e:
            context.log.info(e)
            continue
    return files



def remove_duplicates(remove_duplicates:bool, df:pd.DataFrame, context:OpExecutionContext) -> pd.DataFrame:
    if remove_duplicates:
        before = df.shape[0]
        df = df.drop_duplicates()
        context.add_output_metadata({"Duplicates_removed": MetadataValue.md(str(before))}) 
        #return df
    return df

def remove_timeseries_duplicates(remove_timeseries_duplicates:dict, df:pd.DataFrame) -> pd.DataFrame:
    """
    Remove timeseries duplicates from the dataframe. TimeSeries duplicate are rows that how different timestamp, but
    value stay the same for many rows. In some cases these rows counts as duplicates. Method makes sure that only one copy
    of this kind of row presents in the row. 
    """
    if remove_timeseries_duplicates:
        obj = remove_timeseries_duplicates
        duplicated_value, value_base = obj.get("value_col"), obj.get("base_col")
        df = df[df[duplicated_value] != df.groupby(value_base)[duplicated_value].shift()]
        #return df
    return df

def drop_columns(drop_cols:List[str], df:pd.DataFrame) -> pd.DataFrame:
    if drop_cols:
        df = df.drop(columns=drop_cols)
    return df