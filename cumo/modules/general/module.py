from typing import List, Literal, Optional
import pandas as pd
from dagster import MetadataValue, OpExecutionContext, asset, AssetsDefinition
from pydantic import BaseModel
from dagster_factory_pipelines import ModuleBase
from dagster_factory_pipelines.factory.base import ModuleBase
from dagster_factory_pipelines.factory.registry import register_module
from .helpers import convert_to_stream, read_csvs_from_local_pd, read_csvs_from_minio, write_to_csv_file, remove_duplicates, remove_timeseries_duplicates, drop_columns
from ...utils.utils import df_info, df_rows, timed_asset, asset_len
class TimeSeriesDuplicate(BaseModel):
    base_col:str
    value_col:str

class Minio(BaseModel):
    bucket: str

class ReadAll(BaseModel):
    #key:str
    elements: List[str]
    save: Optional[str] = None

@register_module("write_to_csv")
class write_to_csv(ModuleBase):
    """
    Writes list of dictionaries to CSV file. Saves to localfilesystem or uploads to S3 bucket.

    Supports file_name pk syntax.
    """
    minio: Optional[Minio] = None
    file_name: str

    def create_asset(self) -> AssetsDefinition:
            @asset(
            description="Writes list of dictionaries to the csv",
            compute_kind="CSV",
            required_resource_keys={'minio'} if self.minio else None,
            **self.asset_args
            )
            @timed_asset
            def write_to_csv(context:OpExecutionContext, data:list[dict]) -> None:
                context.log.info(data)
                if self.partition:
                    pk = context.partition_key.keys_by_dimension
                    self.file_name = self.file_name.format(**pk)

                if self.minio:
                    buffer = convert_to_stream(data)
                    context.resources.minio.upload_obj(self.minio.bucket, self.file_name, buffer)
                    return

                write_to_csv_file(self.file_name, data)
            return write_to_csv 

@register_module("read_csv")
class read_csv(ModuleBase):

    minio:Optional[Minio] = None
    read_all: Optional[ReadAll] = None
    file_name: str
    remove_timeseries_duplicates: Optional[TimeSeriesDuplicate] = None
    remove_duplicates: Optional[bool] = False
    drop_cols: Optional[List[str]] = None
    read_all:Optional[ReadAll] = None

    df:pd.DataFrame = None
    model_config = {"arbitrary_types_allowed": True}

    def create_asset(self) -> AssetsDefinition:
            """
            Reads csv file into pandas dataframe. Can read from local files or from S3 bucket, based on user configuration.
            
            Returns dataframe 
            """
            @asset(
            description="Extract data",
            compute_kind="CSV",
            required_resource_keys={'minio'} if self.minio else None,
            **self.asset_args
            )
            @timed_asset
            @df_rows
            @df_info
            def read_csv_pandas(context:OpExecutionContext) ->  pd.DataFrame:
                self.create_pk(context) # creates partition keys and adds to self.pk if partition exists
                if self.minio:
                    self.__read_from_bucket(context)
                else:
                    self.__read_csv_from_local(context)
                self.df = self.df.dropna()
                context.add_output_metadata({"Init_shape": MetadataValue.md(str(self.df.shape))})  
                self.__remove_duplicates(context)
                self.__drop_columns()
                self.__remove_timeseries_duplicates()
                return self.df
            return read_csv_pandas

    def __read_all_bucket_files(self, context:OpExecutionContext) -> list:
        """
        Reads all files from the S3 bucket, if real_all.elements present.
        This method creates dataframe for each read CSV and returns a list of dataframes
        """
        context.log.info("Reading all files")
        files = []
        for el in self.read_all.elements:
            file_name = self.file_name.format(key=el, **self.pk)
            context.log.info(file_name)
            try:
                res = context.resources.minio.get_obj(self.minio.bucket, file_name)
                files.append(pd.read_csv(res))
            except Exception as e:
                context.log.info(e)
                continue
        return files
    def __remove_timeseries_duplicates(self) -> None:
        """
        Remove timeseries duplicates from the dataframe. TimeSeries duplicate are rows that how different timestamp, but
        value stay the same for many rows. In some cases these rows counts as duplicates. Method makes sure that only one copy
        of this kind of row presents in the row. 
        """
        if self.remove_timeseries_duplicates:
            obj = self.remove_timeseries_duplicates.model_dump()
            duplicated_value, value_base = obj.get("value_col"), obj.get("base_col")
            self.df = self.df[self.df[duplicated_value] != self.df.groupby(value_base)[duplicated_value].shift()]

    def __drop_columns(self) -> None:
        if self.drop_cols:
            self.df = self.df.drop(columns=self.drop_cols)

    def __remove_duplicates(self, context:OpExecutionContext) -> None:
        if self.remove_duplicates:
            before = self.df.shape[0]
            self.df = self.df.drop_duplicates()
            context.add_output_metadata({"Duplicates_removed": MetadataValue.md(str(before))}) 
    
    def __save_to_bucket(self, context:OpExecutionContext) -> None:
        
        if self.read_all.save:
            context.log.info("Saving merged data to minio")
            save_file_name = self.read_all.save.format(**self.pk)
            context.resources.minio.upload_obj(self.minio.bucket, save_file_name, self.df.to_csv(index=False).encode('utf-8'))
    
    def __read_csv_from_bucket(self, context:OpExecutionContext) -> None:
        """
        Reads CSV file from the bucket to the dataframe. As a result self.df created.
        """
        self.file_name = self.file_name.format(**self.pk)
        res = context.resources.minio.get_obj(self.minio.bucket, self.file_name)
        self.df = pd.read_csv(res)

    def __read_csv_from_local(self, context:OpExecutionContext) -> None:
        self.file_name = self.file_name.format(**self.pk)
        context.log.info("reading csv from localfiles")
        self.df = pd.read_csv(
            self.file_name,
            sep=","
            )
        
    def __read_from_bucket(self, context:OpExecutionContext):
        """
        Wrapper for all methods related to the minio/S3 bucket
        """
        context.log.info("Reading csv from minio")
        if self.read_all:
            context.log.info("reading all csvs")
            files = self.__read_all_bucket_files(context)
            self.df = pd.concat(files, ignore_index=True)
            self.__save_to_bucket(context)
        else:
            self.__read_csv_from_bucket(context)

@register_module("pandas_ops")
class pandas_ops(ModuleBase):
    commands: List[str]
    def create_asset(self) -> AssetsDefinition:
        """
        Allows to work directly with df without mutating via new variables
        df = df ** something will not work
        """
        @asset(
        description="Extract data",
        compute_kind="pandas",
        **self.get_custom_asset_args("df")
        )
        @df_info
        def pandas_ops(context:OpExecutionContext, df:pd.DataFrame) ->  pd.DataFrame:
            index = 0 
            while index != len(self.commands):
                print(self.commands[index])
                exec(self.commands[index])
                context.log.info(df)
                index += 1
            return df
        return pandas_ops

@register_module("pivot")
class pivot(ModuleBase):

    pivot_params: dict

    def create_asset(self) -> AssetsDefinition:
        """
        Creates pivot table from the dataframe based on users configuration.
        pivot_params = {
            "index":'timestamp',  # Group by time
            "columns":'unit',  # Move unit to columns
            "values":'value',  # Fill values from this column
            "aggfunc":'sum'
        }
        """
        @asset(
        description="Extract data",
        compute_kind="pandas",
        **self.get_custom_asset_args("df")
        )
        @df_info
        def create_pivot(context:OpExecutionContext, df:pd.DataFrame) -> pd.DataFrame:
            result = df.pivot_table(**self.pivot_params).reset_index()
            return result
        return create_pivot

@register_module("df_to_dict")
class df_to_dict(ModuleBase):
    def create_asset(self) -> AssetsDefinition:
        """
        Converts dataframe to list of dicts where object keys are columns and values are rows values
        """
        @asset(
        description="Extract data",
        compute_kind="pandas",
        **self.get_custom_asset_args("df")
        )        
        def df_to_dict(context:OpExecutionContext, df:pd.DataFrame) -> list[dict]:
            return df.to_dict(orient="records")
        return df_to_dict


@register_module("rename_df_cols")    
class rename_df_cols(ModuleBase):

    columns:dict

    def create_asset(self) -> AssetsDefinition:
        """
        Renames dataframe columns
        """
        @asset(
        description="Renames pandas columns",
        compute_kind="pandas",
        **self.get_custom_asset_args("df")
        )        
        def rename_df_col(context:OpExecutionContext, df:pd.DataFrame) -> pd.DataFrame:
            print(self.columns)
            df = df.rename(columns=self.columns)
            return df
        return rename_df_col
    
@register_module("remove_rows")        
class remove_rows(ModuleBase):

    conditions: list[dict]

    def create_asset(self) -> AssetsDefinition:
        """
        Removes rows based on user conditions
        """
        @asset(
        description="Renames pandas columns",
        compute_kind="pandas",
        **self.get_custom_asset_args("df")
        )
        @df_rows
        @df_info    
        def remove_rows(context:OpExecutionContext, df: pd.DataFrame) -> pd.DataFrame:
            for condition in self.conditions:
                cond_obj = condition
                context.log.info(cond_obj)
                if cond_obj.get("condition") == "contains": #TODO it is always contains
                    for value in cond_obj.get("values"):
                        df = df[~df[cond_obj.get("col")].str.contains(value, case=False, na=False)]
            return df
        return remove_rows
@register_module("change_names")  
class change_names(ModuleBase):

    names: list[dict]
    col: str

    def create_asset(self) -> AssetsDefinition:
        """
        Renames certain values in dataframe based on users requirements
        """
        @asset(
        description="Renames pandas columns",
        compute_kind="pandas",
        **self.get_custom_asset_args("df")
        )    
        @df_info
        def change_names(context:OpExecutionContext, df:pd.DataFrame) -> pd.DataFrame:
            for n in self.names:
                contains_me = n.get("name") if not n.get("contains") else n.get("contains")
                contains_me = contains_me.lower()
                df[self.col] = df[self.col].apply(lambda x: n.get("name") if contains_me in x.lower() else x )

            context.add_output_metadata({
                "df": MetadataValue.md(df.head(10).to_markdown()),
                "unique_name": list(df[self.col].unique())})
            return df
        return change_names
    
@register_module("pd.date_aggregator")
class Aggregator(ModuleBase):

    date: str # date col name
    freq: str = "1H" # freq of agg
    value: str # value col name
    value_agg_method: Literal["sum", "max", "min", "mean"]

    def create_asset(self) -> AssetsDefinition:
        @asset(
            description="Aggregates received based on val",
            compute_kind="pandas",
            **self.get_custom_asset_args("df")
        )
        def agg(context: OpExecutionContext, df:pd.DataFrame) -> pd.DataFrame:
            # GPT solution modified
            df[self.date] = pd.to_datetime(df[self.date])
            df.set_index(self.date, inplace=True)
            cols = list(df.columns)
            cols.remove(self.value)
            result = (
                df.groupby([pd.Grouper(freq=self.freq), *cols])
                .agg({self.value: self.value_agg_method})
                .reset_index()
            )
            return  result
        return agg
    
@register_module("ld.date_aggregator")
class Aggregator2(ModuleBase):

    date: str # date col name
    freq: str = "1H" # freq of agg
    value: str # value col name
    value_agg_method: Literal["sum", "max", "min", "mean"]

    def create_asset(self) -> AssetsDefinition:
        @asset(
            description="Aggregates received based on val",
            compute_kind="pandas",
            **self.asset_args
        )
        @asset_len
        def agg(context: OpExecutionContext, data:list[dict]) -> list[dict]:
            # GPT solution modified
            df = pd.DataFrame(data)
            context.log.info(df)
            df[self.date] = pd.to_datetime(df[self.date])
            df.set_index(self.date, inplace=True)
            cols = list(df.columns)
            cols.remove(self.value)
            result = (
                df.groupby([pd.Grouper(freq=self.freq), *cols])
                .agg({self.value: self.value_agg_method})
                .reset_index()
            )
            return  result.to_dict(orient="records")
        return agg
    
@register_module("ld.pandas_ops")
class pandas_ops2(ModuleBase):
    commands: List[str]
    def create_asset(self) -> AssetsDefinition:
        """
        Pandas ops
        """
        @asset(
        description="Executes provided pandas commands",
        compute_kind="pandas",
        **self.asset_args
        )
        @asset_len
        def pandas_ops(context:OpExecutionContext, data:list[dict]) ->  list[dict]:
            df = pd.DataFrame(data)
            for command in self.commands:
                exec(command)
            context.log.info(df)
            return df.to_dict(orient="records")
        return pandas_ops