from abc import abstractmethod


class BaseDeployment:
    """Abstract BaseDeployment class
    It serves as a base  class for all the deployment classes for particular clusters

    """

    @abstractmethod
    async def authentication(self, config_object: object,  init_phase_commands: list, path_function: str):
        pass

    @abstractmethod
    async def deploy(self, config_object: object, path_directory: str) -> str:
        pass

    @abstractmethod
    async def delete(self, config_object: object, path_directory: str) -> str:
        pass