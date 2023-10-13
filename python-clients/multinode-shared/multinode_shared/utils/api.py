from multinode_shared.api_client import ApiClient, Configuration, DefaultApi
from multinode_shared.config import Config


def get_authenticated_client(multinode_config: Config) -> DefaultApi:
    client_config = Configuration(
        host=multinode_config.api_url, access_token=multinode_config.api_key
    )
    client = ApiClient(client_config)
    return DefaultApi(client)
