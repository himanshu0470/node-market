from pyaml_env import parse_config
from score.settings.base import APP_PROPS_PATH

class ApplicationProperties:
    # Class variable to hold the singleton instance
    _instance = None

    def __new__(cls, *args, **kwargs):
        """
        Override the __new__ method to ensure only one instance of the class is created.
        """
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            # Initialize _config attribute to hold the parsed configuration
            cls._instance._config = None
        return cls._instance

    @classmethod
    def load(cls, config_file_path):
        """
        Class method to load the YAML configuration file.
        
        Args:
            config_file_path (str): Path to the YAML configuration file.
        """
        # Parse the YAML configuration file and store it in _config attribute        
        cls._instance._config = parse_config(config_file_path)

        return cls._instance._config

    @classmethod
    def get(cls, key, default=None):
        """
        Class method to get a property value from the loaded configuration.
        
        Args:
            key (str): Key of the property.
            default: Default value to return if the key is not found.
        
        Returns:
            The value corresponding to the key if found, otherwise the default value.
        
        Raises:
            ValueError: If configuration is not loaded.
        """
        # Check if configuration is loaded
        if cls._instance is None or cls._instance._config is None:
            raise ValueError("Configuration not loaded")
        # Return the value corresponding to the key, or the default value if key not found
        return cls._instance._config.get(key, default)


app_props = ApplicationProperties().load(APP_PROPS_PATH)