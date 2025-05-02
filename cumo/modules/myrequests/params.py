from pydantic import BaseModel
from typing import Optional
class ParamAuth(BaseModel):
    name: str
    token: str

class BasicAuth(BaseModel):
    username: str
    password: str

class BearerToken(BaseModel):
    token: str

class APIKey(BaseModel):
    key: str
    key_name: str

class Auth(BaseModel):
    basic_auth: Optional[BasicAuth] = None
    param_token: Optional[ParamAuth] = None
    bearer_token: Optional[BearerToken] = None
    api_key: Optional[APIKey] = None
