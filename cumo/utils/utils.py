from datetime import datetime, timedelta
from functools import wraps
from dagster import MetadataValue
import re, time

def get_range(partition_key: str, delta:int):
    start_date = datetime.strptime(partition_key, "%Y-%m-%d")
    end_date = start_date + timedelta(days=delta)
    return str(start_date).replace(" ", "T")+"Z", str(end_date).replace(" ", "T")+"Z"

def get_hourly_time(partition_key:str):
    pattern = r'(\d{4}-\d{2}-\d{2})-(\d{2})'
    date, hour = re.search(pattern, partition_key).groups()
    date_from = date + f"T{hour}:00:00Z"
    date_to = date+ f"T{hour}:59:59Z"

    return date_from, date_to


def get_daily_time(partition_key:str):
    date_from = partition_key + "T00:00:00Z"
    date_to = partition_key + "T23:59:59Z"
    return date_from, date_to

def get_range_based_on_type(partition_type:str, partition_key:str):
    if "HourlyPartitionsDefinition" in partition_type:
        return get_hourly_time(partition_key)
    
    if "DailyPartitionsDefinition" in partition_type:
        return get_daily_time(partition_key)

    if "WeeklyPartitionsDefinition" in partition_type:
        return get_range(partition_key, 7)

    if "MonthlyPartitionsDefinition" in partition_type:
        return get_range(partition_key, 30)


def timed_asset(func):
    @wraps(func)
    def wrapper(context, *args, **kwargs):
        start_time = time.time()
        result = func(context, *args, **kwargs)
        duration = time.time() - start_time
        context.add_output_metadata({"influx_duration": duration})
        return result
    return wrapper

def asset_len(func):
    @wraps(func)
    def wrapper(context, *args, **kwargs):
        result = func(context, *args, **kwargs)
        context.add_output_metadata({"influx_asset_len": len(result)})
        return result
    return wrapper

def df_rows(func):
    @wraps(func)
    def wrapper(context, *args, **kwargs):
        df = func(context, *args, **kwargs)
        context.add_output_metadata({"influx_dataframe_rows": df.shape[0]})
        return df
    return wrapper

def df_info(func):
    @wraps(func)
    def wrapper(context, *args, **kwargs):
        df = func(context, *args, **kwargs)
        context.add_output_metadata({"tabular_data": MetadataValue.md(df.head(10).to_markdown())})        
        context.add_output_metadata({"df_describe": MetadataValue.md(df.describe().to_markdown())})  
        context.add_output_metadata({"shape": MetadataValue.md(str(df.shape))})  
        return df
    return wrapper