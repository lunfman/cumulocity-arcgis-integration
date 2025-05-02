from typing import List, Optional
import pandas as pd
from dagster import  AssetsDefinition, OpExecutionContext, asset
from dagster_factory_pipelines.factory.base import AssetCheckBase, ModuleBase
from dagster_factory_pipelines.factory.registry import register_check, register_module

@register_module("pd.nan_check")
class dataframe_nan_checks_asset(ModuleBase):
    """
    Validates that required dataframe columns do not contain nan values (Asset based option)
    """
    cols: List[str]

    def create_asset(self)->AssetsDefinition:
        @asset(
            **self.asset_args,
            description='Validates selected dataframe columns against Nan values'
        )
        def dataframe_nan_checks(context:OpExecutionContext, data:pd.DataFrame) -> pd.DataFrame:
            for col in self.cols:
                nans_found = data[col].isna().sum()
                assert(nans_found > 0)
            return data
        return dataframe_nan_checks

@register_module("ld.nan_check")
class dataframe_nan_checks_asset(ModuleBase):
    """
    Validates that required dataframe columns do not contain nan values (Asset based option)
    """
    cols: List[str]
    rows: Optional[int] = None

    def create_asset(self)->AssetsDefinition:
        @asset(
            **self.asset_args,
            description='Validates selected dataframe columns against Nan values'
        )
        def dataframe_nan_checks(context:OpExecutionContext, data:list[dict]) -> None:
            df = pd.DataFrame(data)
            self.__validate_rows_number(df)
            for col in self.cols:
                nans_found = df[col].isna().sum()
                context.log.debug(f"Nans found: {nans_found}")
                assert(nans_found == 0)
        return dataframe_nan_checks


    def __validate_rows_number(self, df:pd.DataFrame) -> None:
        """
        Validates amount of rows
        """
        if self.rows:
            assert len(df) <= self.rows