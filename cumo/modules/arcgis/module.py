import asyncio
import json
import os
from typing import List, Optional
import aiohttp
from dagster import AssetsDefinition, ConfigurableResource, OpExecutionContext, asset, get_dagster_logger
import dagster
import requests

from dagster_factory_pipelines import ModuleBase
from ...utils.utils import timed_asset, asset_len
from dateutil import parser

from dagster_factory_pipelines.factory.base import ModuleBase
from dagster_factory_pipelines.factory.registry import register_module, register_resource

logger = get_dagster_logger()

@register_resource("ArcGIS")
class ArcGIS(ConfigurableResource):
    """
    ArcGIS resource used for interaction with ArcGIS REST API.

    token: required (token with valid access)
    feature_service_address: required (valid feature service)
    """
    token:str # as env variable other wise exposed
    feature_service_address:str # services6.arcgis.com/P9oWcU3j68LVOKFp
    hostname: str = "tartu.maps.arcgis.com"
    skip_validation:bool = False

    def model_post_init(self, ctx):
        """
        Check that credentials are valid
        """
        
        if self.skip_validation:
            return

        url = f"https://{self.hostname}/sharing/rest/community/self"
        params = {
            "f": "pjson",
            "token": os.getenv(self.token)
        }
        res = requests.get(url=url, params=params)
        print(res.text)
        if "Invalid token" in res.text:
            raise dagster.DagsterError("ArcGIS token is invalid, provide the correct token")
        
        if "error" in res.text:
            raise dagster.DagsterError("Can not validate ArcGIS token. Provide valid ArcGIS token")

    async def add_feature_async(self, session:aiohttp.ClientSession, layer_name:str, sublayer_id:int, obj:dict):
        """
        Adds features to ArcGIS server in a asynchronous way
        """
        params = {
            "f": "json",
            "features": json.dumps(obj, indent=4, sort_keys=True, default=str),
            "token": os.getenv(self.token)
        }
        url = f"https://{self.feature_service_address}/arcgis/rest/services/{layer_name}/FeatureServer/{sublayer_id}/addFeatures"
        async with session.post(url, params=params) as res:
            data = await res.read()
            response_json = json.loads(data)
            return response_json


    # def create_column_in_table(self, layer_name:str, sublayer_id:int, col_name:str, field_type="esriFieldTypeDouble") -> None:
    #     """
    #     Creates column in ArcGIS table
    #     """
    #     url = f"https://{self.feature_service_address}/arcgis/rest/admin/services/{layer_name}/FeatureServer/{sublayer_id}/addToDefinition"
    #     obj = {"fields":[{"name":col_name,"type":field_type,"alias":col_name,"nullable":True,"editable":True}]}

    #     params = {
    #         "f": "json",
    #         "token": os.getenv(self.token),
    #         "addToDefinition": json.dumps(obj)
    #     }

    #     res = requests.post(url, params=params)
    #     logger.info(res.text)
    #     logger.info(res.status_code)

    def get_layer_cols(self, layer_name:str, sublayer_id:int) -> List[str]:
        """
        Retrieves existing column names for layer_name with sublayer_id
        """
        url = f"https://{self.feature_service_address}/arcgis/rest/services/{layer_name}/FeatureServer/{sublayer_id}"
        params = {
            "f": "json",
            "token": os.getenv(self.token)
        }
        res = requests.get(url, params=params).json()
        return res["fields"]

    def get_layers(self, layer_name:str) -> List[str]:
        """
        Retrieves existing sublayers for the layer_name
        """
        url = f"https://{self.feature_service_address}/arcgis/rest/services/{layer_name}/FeatureServer"
        params = {
            "f": "json",
            "token":os.getenv(self.token)
        }
    
        res = requests.get(url, params=params)
        logger.info(res.status_code)
        return res.json()["layers"]


class validate_tables(ModuleBase):
    """
    Validates if columns in table
    """

    column_names: list

    def validate(self):
        @asset(
            **self.args,
            description="Checks are columns created in ArcGIS"
        )
        @timed_asset
        def validate_table(context:OpExecutionContext, arcGIS:ArcGIS) -> AssetsDefinition:
            pass

@register_module('transform_to_argcis_format')
class transform_to_argcis_format(ModuleBase):
## create_asset

    lng:str = "lng"
    lat:str = "lat"
    def create_asset(self) -> AssetsDefinition:
        """
        Transforms list of dictionaries to ArcGIS  JSON format

        x and y (ArcGIS coordinates) can be found via lat and lng dictionary keys. It means if you want to send them to arcgis
        the lat and lng keys should exists in the object, otherwise these values will be empty
        """
        @asset(
        **self.asset_args,
        description="Extract data",
        compute_kind="python",
        )
        @asset_len
        @timed_asset
        def transform_to_arcgis_format(context:OpExecutionContext, data:List[dict]) -> List:
            res = []
            for measurement in data:
                res.append(
                    {
                        "attributes":measurement,
                        "geometry": {
                            "x": measurement.pop(self.lng, None),
                            "y": measurement.pop(self.lat, None),
                            "spatialReference": { "wkid": 4326},
                        }
                    }

                    )
            context.log.info(res)
            return res
        return transform_to_arcgis_format
    
@register_module('send_to_arcgis')
class send_to_arcgis(ModuleBase):

    layer_name: str
    sublayer_name: str
    create_cols: Optional[bool] = False
    create_sublayer: Optional[bool] = False
    col_types: Optional[dict] = None
    req_cols: Optional[List] = None
    
    # class var
    sublayer_id: str = ""

    def create_asset(self) -> AssetsDefinition:
            @asset(
                    **self.asset_args,
                    description="Upload data to sql db",
                    compute_kind="SQL",
                )
            @timed_asset
            def load_cumu_data(context:OpExecutionContext, arcGIS:ArcGIS, data:List[dict]) -> None:
                self.__create_sublayers(arcGIS)
                self.__get_sublayer_id(arcGIS)
                self.__validate_table(arcGIS, data[0])

                context.log.info(self.sublayer_id)
                context.log.info(self.layer_name)
                context.log.info(self.sublayer_name)

                #self.__create_arcgis_table(context, arcGIS, data)
                # validate that columns exists
                # check first object


                asyncio.run(self.__main(context, arcGIS, data))
            return load_cumu_data
    

    async def __main(self, context:OpExecutionContext, arcGIS:ArcGIS, data:List[dict]):
        """
        Creates async tasks and execute them
        """

        async with aiohttp.ClientSession() as session:

            tasks = []
            for feat in data:
                tasks.append(asyncio.ensure_future(
                    arcGIS.add_feature_async(
                        session,
                        self.layer_name,
                        self.sublayer_id,feat
                        )
                    )
                )

            arcgis_res = await asyncio.gather(*tasks)
            for arcgis_res in arcgis_res:
                context.log.info(arcgis_res)


    def __create_arcgis_table(self, context:OpExecutionContext, arcGIS:ArcGIS, data:List[dict]) -> None:
        """
        Create missing columns in ArcGIS
        """
        if self.create_cols:
            layer_cols = arcGIS.get_layer_cols(self.layer_name, self.sublayer_id)
            col_names = [col["name"] for col in layer_cols]
            cur_cols = list(data[0]["attributes"].keys())
            context.log.info(col_names)
            context.log.info(cur_cols)
            for name in cur_cols:
                if name not in col_names:
                    context.log.info("Create col")
                    if name in self.col_types:
                        arcGIS.create_column_in_table(self.layer_name, self.sublayer_id, name, field_type=self.col_types[name])
                    else:
                        arcGIS.create_column_in_table(self.layer_name, self.sublayer_id, name)

    def __create_sublayers(self, arcGIS:ArcGIS) -> None:
        """
        Creates missing sublayer in ArcGIS. Works with a privileged token only.
        """
        if self.create_sublayer:
            layers = arcGIS.get_layers(self.layer_name)
            layer_names = [layer["name"] for layer in layers]

            if self.sublayer_name not in layer_names:
                arcGIS.create_layer(self.layer_name, self.sublayer_name)

    def __get_sublayer_id(self, arcGIS:ArcGIS) -> None:
            """
            Gets and sets current layer id
            """
            self.sublayer_id = [layer["id"] for layer in arcGIS.get_layers(self.layer_name) if self.sublayer_name == layer["name"]][0]


    def __validate_table(self, arcGIS:ArcGIS, data:dict) -> None:
        """
        Checks if table columns present in ArcGIS
        """
        layer_cols = arcGIS.get_layer_cols(self.layer_name, self.sublayer_id)
        arcgis_cols = [col["name"] for col in layer_cols]
        data_cols = self.req_cols if self.req_cols else list(data["attributes"].keys())
        for col in data_cols:
            if col not in arcgis_cols:
                raise Exception(f"Column {col} does not exists in ArcGIS table")

        return
@register_module('correct_timestamps')
class correct_timestamps(ModuleBase):

    timestamp_key: str
    
    def create_asset(self) -> AssetsDefinition: # TODO remove it
        """
        Transforms timestamp to ArcGIS compatible
        """
        @asset(
        **self.asset_args,
        compute_kind="SQL",
        description="Upload data to sql db"
        )
        def correct_timestamps(context:OpExecutionContext, data):
            for feat in data:
                feat[self.timestamp_key] = int(parser.parse(feat[self.timestamp_key]).timestamp() * 1000)
            return data
        return correct_timestamps


