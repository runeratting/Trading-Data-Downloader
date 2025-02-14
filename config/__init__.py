"""
Configuration Management for Trading Data Downloader

This module provides a robust configuration system that supports:
- Environment-specific configuration (development, production)
- Configuration validation
- Environment variable overrides
- Typed configuration objects

Usage:
    from config import load_config
    
    # Load configuration (uses TRADING_ENV environment variable, defaults to 'development')
    config = load_config()
    
    # Access configuration
    db_host = config.db.host
    buffer_size = config.buffer.buffer_size
    
    # Setup logging
    config.setup_logging()

Environment Variables:
    TRADING_ENV: Current environment ('development' or 'production')
    DB_HOST: Database host
    DB_NAME: Database name
    DB_USER: Database user
    DB_PASSWORD: Database password
    DB_PORT: Database port

Configuration Files:
    base.yml: Base configuration for all environments
    development.yml: Development environment overrides
    production.yml: Production environment overrides (optional)
"""

from .config_schema import (
    Config,
    DatabaseConfig,
    InstrumentConfig,
    DownloaderConfig,
    DatabaseBufferConfig,
    LoggingConfig,
)
from .config_loader import ConfigLoader, ConfigurationError

def load_config(config_dir: str = 'config') -> Config:
    """
    Load and validate configuration for the current environment.
    
    Args:
        config_dir: Directory containing configuration files (default: 'config')
        
    Returns:
        Config: Validated configuration object
        
    Raises:
        ConfigurationError: If configuration loading or validation fails
    """
    loader = ConfigLoader(config_dir)
    return loader.load_config()

__all__ = [
    'load_config',
    'Config',
    'DatabaseConfig',
    'InstrumentConfig',
    'DownloaderConfig',
    'DatabaseBufferConfig',
    'LoggingConfig',
    'ConfigurationError',
]
