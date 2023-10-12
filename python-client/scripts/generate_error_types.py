import json
import sys

error_types_path = sys.argv[1]

with open(error_types_path, "r") as file:
    error_jsons_list = json.loads(file.read())

generated_code_path = "./multinode/api_client/error_types.py"

TEMPLATE = """from multinode.api_client import ApiException
import json

{error_classes}
def resolve_error(original: ApiException) -> BaseException:
    status = original.status
    detail = json.loads(original.body).get('detail')
{if_statements}
    return original
"""

ERROR_TYPE_TEMPLATE = """
class {error_name}(BaseException):
    pass

"""

RESOLVE_ERROR_IF_STATEMENT_TEMPLATE = """
    if status == {error_status} and detail == '{error_message}':
        return {error_name}()
"""

with open(generated_code_path, "w") as file:
    error_classes = []
    for error_json in error_jsons_list:
        error_classes.append(
            ERROR_TYPE_TEMPLATE.format(error_name=error_json["error_name"])
        )

    if_statements = []
    for error_json in error_jsons_list:
        if_statements.append(
            RESOLVE_ERROR_IF_STATEMENT_TEMPLATE.format(
                error_name=error_json["error_name"],
                error_status=error_json["error_status_code"],
                error_message=error_json["error_message"],
            )
        )

    file.write(
        TEMPLATE.format(
            error_classes="".join(error_classes), if_statements="".join(if_statements)
        )
    )
