from abc import abstractmethod
from pandas import DataFrame


class BaseCollector:
    """Abstract DataCollector class
    It serves as a base  class for all the deployment classes for particular clusters

    """

    @abstractmethod
    async def collect(self, config_object: object, cluster_name: str, start: int, end: int) -> DataFrame:
        pass

    @abstractmethod
    def do_frame_postprocessing(self, frame: DataFrame, target_name: str, measurement_category: str) -> DataFrame:
        pass