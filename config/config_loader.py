import os
from pathlib import Path
from typing import Dict, Any
import yaml
import logging
from datetime import datetime
from .config_schema import (
    Config,
    DatabaseConfig,
    InstrumentConfig,
    DownloaderConfig,
    DatabaseBufferConfig,
    LoggingConfig
)

class ConfigurationError(Exception):
    """Raised when there's an error in configuration loading or validation"""
    pass

class ConfigLoader:
    """Loads and validates configuration from YAML files"""

    def __init__(self, config_dir: str = 'config'):
        self.config_dir = Path(config_dir)
        self.environment = os.getenv('TRADING_ENV', 'development')

    def load_config(self) -> Config:
        """Load configuration for current environment"""
        try:
            # Load base config
            base_config = self._load_yaml('base.yml')
            
            # Load environment specific config
            env_config = self._load_yaml(f'{self.environment}.yml')
            
            # Merge configurations
            merged_config = self._merge_configs(base_config, env_config)
            
            # Create configuration objects
            config = self._create_config(merged_config)
            
            # Validate configuration
            config.validate()
            
            return config
            
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {str(e)}")

    def _load_yaml(self, filename: str) -> Dict[str, Any]:
        """Load YAML configuration file"""
        file_path = self.config_dir / filename
        if not file_path.exists():
            if filename.startswith(self.environment):
                # Environment specific config is optional
                return {}
            raise ConfigurationError(f"Configuration file not found: {file_path}")
            
        try:
            with open(file_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            raise ConfigurationError(f"Error loading {filename}: {str(e)}")

    def _merge_configs(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two configuration dictionaries"""
        result = base.copy()
        
        def deep_merge(target, source):
            for key, value in source.items():
                if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                    deep_merge(target[key], value)
                else:
                    target[key] = value
        
        deep_merge(result, override)
        return result

    def _create_config(self, config_dict: Dict[str, Any]) -> Config:
        """Create Config object from dictionary"""
        try:
            # Create database config
            db_config = DatabaseConfig(
                host=self._get_env_or_default('DB_HOST', config_dict['database']['host']),
                database=self._get_env_or_default('DB_NAME', config_dict['database']['name']),
                user=self._get_env_or_default('DB_USER', config_dict['database']['user']),
                password=self._get_env_or_default('DB_PASSWORD', config_dict['database']['password']),
                port=int(self._get_env_or_default('DB_PORT', config_dict['database']['port']))
            )

            # Create instruments config
            instruments = {}
            for symbol, instr_config in config_dict['instruments'].items():
                instruments[symbol] = InstrumentConfig(
                    symbol=symbol,
                    point_value=instr_config['point_value'],
                    table_name=instr_config['table_name'],
                    start_date=datetime.strptime(instr_config['start_date'], '%Y-%m-%d'),
                    trading_hours=self._import_trading_hours(instr_config['trading_hours'])
                )

            # Create optional configs
            downloader = None
            if 'downloader' in config_dict:
                downloader = DownloaderConfig(
                    max_retries=config_dict['downloader'].get('max_retries', 3),
                    initial_retry_delay=config_dict['downloader'].get('initial_retry_delay', 2.0),
                    max_concurrent_downloads=config_dict['downloader'].get('max_concurrent_downloads', 4),
                    batch_size=config_dict['downloader'].get('batch_size', 4),
                    cache_days=config_dict['downloader'].get('cache_days', 10),
                    user_agent=config_dict['downloader'].get('user_agent', 'Python')
                )

            buffer = None
            if 'buffer' in config_dict:
                buffer = DatabaseBufferConfig(
                    buffer_size=config_dict['buffer'].get('buffer_size', 100000),
                    max_batch_size=config_dict['buffer'].get('max_batch_size', 1000),
                    flush_interval=config_dict['buffer'].get('flush_interval', 60.0)
                )

            logging_config = None
            if 'logging' in config_dict:
                log_cfg = config_dict['logging']
                logging_config = LoggingConfig(
                    level=getattr(logging, log_cfg.get('level', 'INFO')),
                    format=log_cfg.get('format', '%(asctime)s - %(levelname)s - %(message)s'),
                    log_dir=Path(log_cfg.get('log_dir', 'logs')),
                    file_handlers=log_cfg.get('file_handlers')
                )

            return Config(
                db_config=db_config,
                instruments=instruments,
                downloader=downloader,
                buffer=buffer,
                logging=logging_config
            )

        except Exception as e:
            raise ConfigurationError(f"Error creating configuration objects: {str(e)}")

    def _get_env_or_default(self, env_var: str, default: Any) -> Any:
        """Get value from environment variable or return default"""
        return os.getenv(env_var, default)

    def _import_trading_hours(self, function_path: str) -> callable:
        """Import trading hours function from module"""
        try:
            # Special case for config.py in root directory
            if function_path.startswith('config.'):
                import importlib.util
                spec = importlib.util.spec_from_file_location(
                    "config", 
                    str(Path(__file__).parent.parent / "config.py")
                )
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                return getattr(module, function_path.split('.')[1])
            
            # For other modules
            module_path, function_name = function_path.rsplit('.', 1)
            module = __import__(module_path, fromlist=[function_name])
            return getattr(module, function_name)
        except Exception as e:
            raise ConfigurationError(f"Error importing trading hours function: {str(e)}")
