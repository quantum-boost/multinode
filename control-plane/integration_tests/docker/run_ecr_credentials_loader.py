from control_plane.docker.ecr_credentials_loader import (
    EcrContainerRepositoryCredentialsLoader,
)

REPOSITORY_NAME = "921216064263.dkr.ecr.eu-west-2.amazonaws.com/multinode"
AWS_REGION = "eu-west-2"


def main() -> None:
    credentials_loader = EcrContainerRepositoryCredentialsLoader(
        repository_name=REPOSITORY_NAME, aws_region=AWS_REGION
    )

    credentials = credentials_loader.load()

    # No asserts - just check it runs cleanly and print results
    print(credentials)


if __name__ == "__main__":
    main()
