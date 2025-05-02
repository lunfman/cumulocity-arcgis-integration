from typing import List, Optional
from dagster import AssetsDefinition, OpExecutionContext, asset
from pydantic import BaseModel
from dagster_factory_pipelines import ModuleBase, registry
from ...utils.utils import df_info, df_rows, timed_asset, get_range_based_on_type
import pandas as pd

class Minio(BaseModel):
    bucket: str
    file_name: str

class TimeSeriesDuplicate(BaseModel):
    base_col:str
    value_col:str

class ModuleParams(BaseModel):
    minio: Optional[Minio] = None
    sources: Optional[List[str]] = None

@registry.register_module('dict.remove_duplicates')
class remove_duplicates(ModuleBase):

    remove_timeseries_duplicates: Optional[TimeSeriesDuplicate] = None

    """
    Removes duplicates from dictionary using pandas drop duplicates
    """
    def create_asset(self) -> AssetsDefinition:
        @asset(
                description="Removes duplicates from dictionary using pandas",
                kinds=["pandas"],
                **self.asset_args
        )
        def remove_duplicates(context:OpExecutionContext, data:list[dict]) -> list[dict]: 
            df = pd.DataFrame(data).drop_duplicates()
            init_size = df.shape[0]

            if self.remove_timeseries_duplicates:
                duplicated_value, value_base = self.remove_timeseries_duplicates.value_col, self.remove_timeseries_duplicates.base_col
                df = df[df[duplicated_value] != df.groupby(value_base)[duplicated_value].shift()]

            final_size = df.shape[0]
            context.add_output_metadata({
                "duplicates_removed": init_size-final_size,
                "duplicate_ratio": (init_size-final_size)/init_size * 100
                }
                )
            return df.to_dict(orient="records")
        return remove_duplicates
    

    
@registry.register_module('s3.injection')    
class inject_inventory_s3(ModuleBase):
    """
    Injects required values from inventory to the list of dictionaries.
    Uses id on common pattern to detect possible injection value.
    Designed primarily for working with CSV files
    """
    minio: Minio
    key: str
    csv_key: str
    inject_values: dict

    def create_asset(self) -> AssetsDefinition:
        @asset(
            description="Injects values from S3 file",
            kinds=["Minio", "Python"],
            required_resource_keys={'minio'},
            **self.asset_args
        )
        def inject(context:OpExecutionContext, data:list[dict]) -> list[dict]:
            inventory = context.resources.minio.get_obj(self.minio.bucket, self.minio.file_name)
            df = pd.read_csv(inventory)
            print(df)
            for obj in data:
                obj_source = obj.get(self.key)
                try:
                    row = df.loc[df[self.csv_key] == int(obj_source)]
                except ValueError:
                    row = df.loc[df[self.csv_key] == obj_source]
                inject_values(row, obj)

            print(data)
            return data

        def inject_values(row:pd.Series, obj:dict):
            """
            Injects values to dictionary from series
            """
            if row.empty:
                return
            for key, value in self.inject_values.items():
                obj[value] = row.iloc[0][key]
        
        return inject