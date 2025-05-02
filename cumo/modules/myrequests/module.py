
from typing import Optional
from dagster import AssetsDefinition, OpExecutionContext, asset
from pydantic import BaseModel
from datetime import datetime, timezone

from dagster_factory_pipelines import ModuleBase
from dagster_factory_pipelines.factory.base import ModuleBase
from dagster_factory_pipelines.factory.registry import register_module
from .helpers import Request, get_mapping_objs

from ...utils.utils import asset_len, timed_asset, df_info, get_range_based_on_type
import pandas as pd

import jmespath

class APIKey(BaseModel):
    key: str
    key_name: str

class BasicAuth(BaseModel):
    username: str
    password: str

class BearerToken(BaseModel):
    token: str

class Auth(BaseModel):
    basic_auth: Optional[BasicAuth] = None
    bearer_token: Optional[BearerToken] = None
    api_key: Optional[APIKey] = None

class CheckMore(BaseModel):
    condition: str
    parameter: str

class Minio(BaseModel):
    bucket: str
    file_name: str

class ModuleParams(BaseModel):
    page_size: Optional[int] = 2000
    fragment: Optional[bool] = False
    minio: Optional[Minio] = None
    partition_mapping: Optional[dict] = None
    params: Optional[dict] = None
    check_more: Optional[CheckMore] = None
    endpoint: Optional[str] = None
    auth: Optional[Auth] = None
    variables: Optional[dict] = None
    mappings: Optional[dict] = None
    #
    make_objs: Optional[bool] = True
    direct: Optional[bool] = False

@register_module('http_get')
class myrequests(ModuleBase):
    """
    My requests module allows to send API get requests.

    Configuration example

        name: cumo.modules.myrequests
        component: myrequests
        action: get_api_data
        params:
        endpoint: https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}
        variables: # can be any var in {}
            lat: 59.436962
            lon: 24.753574
        auth:
            param_token:
            param_name: appid
            token: <token>

    """
    endpoint:str
    auth: Optional[Auth] = None
    partition_mapping: Optional[dict] = None
    params: Optional[dict] = None
    check_more: Optional[CheckMore] = None
    variables: Optional[dict] = None

    def create_asset(self) -> AssetsDefinition:
        @asset(
        description="Gets data from an API and returns JSON obj",
        compute_kind="python",
        **self.asset_args
        )
        @asset_len
        @timed_asset
        def get_api_data(context:OpExecutionContext) -> list:
            # how to add partition integration?
            params = {}
            self.create_pk(context)
            if self.partition_mapping and self.partition:
                pk = self.pk
                static, date = pk.get("static"), pk.get("date")

                if date:
                    date_from, date_to = get_range_based_on_type(str(type(self.partition.date_partition)), date)
                    d_f, d_t = self.partition_mapping["dates"][0], self.partition_mapping["dates"][1]
            
                    params[d_f] = date_from
                    params[d_t] = date_to

                s_e = self.partition_mapping["elements"]
                params[s_e] = static
                        

            if self.params:
                params.update(self.params)

            if self.check_more:
                params[self.check_more.parameter] = 1

            context.log.info(self.params)
            context.log.info(params)

            if self.check_more:
                res = []
                while True:
                # put while loop
                    context.log.info(params)
                    my_req = Request(self.endpoint, self.variables, self.auth, params)
                    json_data = my_req.get_data().json()
                    res.append(json_data)
                    context.log.info(json_data)
                    context.log.info(len(json_data))
                    if eval(self.check_more.condition):
                        print("I need to check more")
                        print(f"I will use the next param for that {self.check_more.parameter}")
                        params[self.check_more.parameter] += 1
                        continue
                    break
                return res
            else:
                my_req = Request(self.endpoint, self.variables, self.auth, params)
                json_data = my_req.get_data().json()
                context.log.info(json_data)

                return [json_data]
        return get_api_data

@register_module("json_mapper")
class json_mapper(ModuleBase):
    """
    The modules allows extract json data to the python dictionary by providing required mapping
    """

    mappings: dict
    direct: Optional[bool] = False
    make_objs: Optional[bool] = True

    def create_asset(self) -> AssetsDefinition:
        @asset(
        description="Maps json data",
        compute_kind="python",
        **self.asset_args
        )
        @asset_len
        @timed_asset        
        def map_json(context:OpExecutionContext, data):
            context.log.info(data)
            obj_mappings = dict()
            
            context.log.info(self.direct)

            if self.direct:
                for obj in data:
                    for key, value in self.mappings.items():
                        return jmespath.search(value, obj)

            for key in self.mappings:
                obj_mappings[key] = []

            for obj in data:
                for key, value in self.mappings.items():
                    context.log.debug(jmespath.search(value, obj))
                    search_res = jmespath.search(value, obj)
                    if not isinstance(search_res, list):
                        search_res = [search_res]
                    obj_mappings[key] += search_res

            if self.make_objs:
                res = get_mapping_objs(obj_mappings)
                context.log.info(res)
                return res
            context.log.info(obj_mappings)
            return obj_mappings
        return map_json
    
@register_module("questdb.api")
class QuestDbGet(ModuleBase):
    """
    Retrieves data from quest db api and store it as a pandas dataframe
    """

    endpoint: str
    query: str
    # auth staff later

    def create_asset(self) -> AssetsDefinition:
        @asset(
            kinds = ["python"],
            description="Gets data from QuestDB and returns as dataframe",
            **self.asset_args
        )
        @df_info
        def get_quest_db_data(context:OpExecutionContext) -> pd.DataFrame:
            self.create_pk(context)
            context.log.info(f"QuestDB endpoint:{self.endpoint}, Query: {self.query}")
            res = Request(self.endpoint, [], None, params={"query":self.query.format(**self.pk)})
            data = res.get_data()
            print(data.status_code)
            print(data.json())
            data = data.json()
            columns = [col["name"] for col in data["columns"]]
            return pd.DataFrame(pd.DataFrame(data.get("dataset"), columns=columns))
        return get_quest_db_data
    
@register_module("date.timestamp")
class AddTimestampToObjs(ModuleBase):
    """
    Adds a current timestamp to the objects in the list. Timestamp name can be modified.
    """

    timestamp_name:str = "date"

    def create_asset(self) -> AssetsDefinition:
        @asset(
                kinds=["python"],
                **self.asset_args
        )
        def add_timestamp(context:OpExecutionContext, data:list[dict]) -> list[dict]:
            timestamp = datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
            for obj in data:
                obj[self.timestamp_name] = timestamp

            return data
        return add_timestamp