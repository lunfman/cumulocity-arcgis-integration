import requests
import requests.auth
from .params import Auth

class Request:
    """
    Class provides simplify API for python request lib
    """
    def __init__(self, endpoint:str, variables:list, auth:Auth, params=None):
        self.endpoint = endpoint # endpoint of the url
        self.variables = variables # variables that can be injected to the url
        self.auth = auth # the auth method dict
        self.params = params # params ?
        self.create_variable_based_endpoint()
        print(params, self.params)

    def create_variable_based_endpoint(self) -> None:
        """
        Injects provided variables to endpoint if there are any
        """
        if self.variables:
            self.endpoint = self.endpoint.format(**self.variables)

    def get_data(self) -> requests.Response:
        """
        Gets data from the endpoint in json format
        """
        if not self.auth:
            self.res = requests.get(self.endpoint, params=self.params)
            return self.res
        
        if self.auth.basic_auth:
            auth_obj =self.auth.basic_auth
            username, password = auth_obj.username, auth_obj.password
            self.res = requests.get(self.endpoint, params=self.params, auth=requests.auth.HTTPBasicAuth(username, password))
            return self.res
        
        if self.auth.bearer_token:
            headers = {"Authorization": f"Bearer {self.auth.bearer_token.token}"}
            self.res = requests.get(self.endpoint, params=self.params, headers=headers)
            return self.res

        if self.auth.api_key:
            headers = {f"{self.auth.api_key.key_name}": f"{self.auth.api_key.key}"}
            self.res = requests.get(self.endpoint, params=self.params, headers=headers)
            return self.res



def get_mapping_objs(mappings:dict) -> list:
        keys = list(mappings.keys())
        print(keys)
        ls = []
        if isinstance(mappings[keys[0]], list):
            for index, obj in enumerate(mappings[keys[0]]):
                if obj is None:
                    continue
                ob = dict()
                ob[keys[0]] = obj
                for key in keys[1:]:
                    val =  mappings[key][index]
                    if val is None:
                        continue
                    ob[key] = mappings[key][index]
                ls.append(ob)

            return ls
        return mappings