from uuid import uuid4


def generate_random_id(prefix: str) -> str:
    return f"{prefix}-{uuid4().hex}"
