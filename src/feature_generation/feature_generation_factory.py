from src.feature_generation.disney.disney_feature_generation import DisneyFeatureGenration
from src.config_generation.config_factory import DisneyConfig, HuluConfig, XgboostConfig

class FeatureGenerationExecutor:
    config_map = {
        "DISNEY": DisneyFeatureGenration(),
    }

    def __init__(self, service_config:  DisneyConfig | HuluConfig | XgboostConfig):
        self.service_config = service_config

    def execute(self):
        return self.config_map[self.service_config.base_config.service].execute(self.service_config)
