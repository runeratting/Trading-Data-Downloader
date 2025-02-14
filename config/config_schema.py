from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Dict, Optional, Union
import logging
from pathlib import Path

@dataclass
class DatabaseConfig:
    host: str
    database: str
    user: str
    password: str
    port: int

    def validate(self) -> None:
        """Validate database configuration"""
        if not all([self.host, self.database, self.user, self.password]):
            raise ValueError("All database connection parameters must be provided")
        if not isinstance(self.port, int) or not (1 <= self.port <= 65535):
            raise ValueError("Database port must be a valid port number (1-65535)")

@dataclass
class InstrumentConfig:
    symbol: str
    point_value: int
    table_name: str
    start_date: datetime
    trading_hours: Callable[[int, int], bool]

    def validate(self) -> None:
        """Validate instrument configuration"""
        if not self.symbol:
            raise ValueError("Instrument symbol must be provided")
        if not isinstance(self.point_value, int) or self.point_value <= 0:
            raise ValueError("Point value must be a positive integer")
        if not self.table_name:
            raise ValueError("Table name must be provided")
        if not isinstance(self.start_date, datetime):
            raise ValueError("Start date must be a datetime object")
        if not callable(self.trading_hours):
            raise ValueError("Trading hours must be a callable function")

@dataclass
class DownloaderConfig:
    max_retries: int = 3
    initial_retry_delay: float = 2.0
    max_concurrent_downloads: int = 4
    batch_size: int = 4
    cache_days: int = 10
    user_agent: str = 'Python'

    def validate(self) -> None:
        """Validate downloader configuration"""
        if not isinstance(self.max_retries, int) or self.max_retries < 1:
            raise ValueError("Max retries must be a positive integer")
        if not isinstance(self.initial_retry_delay, (int, float)) or self.initial_retry_delay <= 0:
            raise ValueError("Initial retry delay must be a positive number")
        if not isinstance(self.max_concurrent_downloads, int) or self.max_concurrent_downloads < 1:
            raise ValueError("Max concurrent downloads must be a positive integer")
        if not isinstance(self.batch_size, int) or self.batch_size < 1:
            raise ValueError("Batch size must be a positive integer")
        if not isinstance(self.cache_days, int) or self.cache_days < 1:
            raise ValueError("Cache days must be a positive integer")

@dataclass
@dataclass
class ValidationConfig:
    """Configuration for data validation"""
    max_price_spread_percent: float = 10.0  # Maximum allowed spread as percentage
    max_volume: int = 1_000_000  # Maximum allowed volume
    min_tick_interval: float = 0.001  # Minimum time between ticks in seconds
    max_tick_interval: float = 60.0  # Maximum time between ticks in seconds
    log_invalid_ticks: bool = True  # Whether to log invalid ticks
    reject_invalid_ticks: bool = True  # Whether to reject invalid ticks

    def validate(self) -> None:
        """Validate validation configuration"""
        if not isinstance(self.max_price_spread_percent, (int, float)) or self.max_price_spread_percent <= 0:
            raise ValueError("Max price spread percentage must be a positive number")
        if not isinstance(self.max_volume, int) or self.max_volume <= 0:
            raise ValueError("Max volume must be a positive integer")
        if not isinstance(self.min_tick_interval, (int, float)) or self.min_tick_interval <= 0:
            raise ValueError("Min tick interval must be a positive number")
        if not isinstance(self.max_tick_interval, (int, float)) or self.max_tick_interval <= 0:
            raise ValueError("Max tick interval must be a positive number")
        if self.min_tick_interval >= self.max_tick_interval:
            raise ValueError("Min tick interval must be less than max tick interval")

@dataclass
class DatabaseBufferConfig:
    buffer_size: int = 100000
    max_batch_size: int = 1000
    flush_interval: float = 60.0  # seconds

    def validate(self) -> None:
        """Validate buffer configuration"""
        if not isinstance(self.buffer_size, int) or self.buffer_size < 1:
            raise ValueError("Buffer size must be a positive integer")
        if not isinstance(self.max_batch_size, int) or self.max_batch_size < 1:
            raise ValueError("Max batch size must be a positive integer")
        if not isinstance(self.flush_interval, (int, float)) or self.flush_interval <= 0:
            raise ValueError("Flush interval must be a positive number")

@dataclass
class LoggingConfig:
    level: int = logging.INFO
    format: str = '%(asctime)s - %(levelname)s - %(message)s'
    log_dir: Path = Path('logs')
    file_handlers: Dict[str, Dict] = None

    def __post_init__(self):
        if self.file_handlers is None:
            self.file_handlers = {
                'main': {
                    'filename': 'downloader.log',
                    'level': logging.INFO
                },
                'data_issues': {
                    'filename': 'data_issues.log',
                    'level': logging.ERROR
                }
            }

    def validate(self) -> None:
        """Validate logging configuration"""
        if not isinstance(self.level, int):
            raise ValueError("Log level must be an integer")
        if not self.format:
            raise ValueError("Log format must be provided")
        if not isinstance(self.log_dir, Path):
            raise ValueError("Log directory must be a Path object")
        if not isinstance(self.file_handlers, dict):
            raise ValueError("File handlers must be a dictionary")
        for handler in self.file_handlers.values():
            if 'filename' not in handler or 'level' not in handler:
                raise ValueError("Each file handler must have filename and level")

@dataclass
class CheckpointConfig:
    """Configuration for checkpoint system"""
    enabled: bool = True
    directory: Path = Path('checkpoints')
    cleanup_days: int = 30
    auto_resume: bool = True
    save_interval: int = 1  # Save state every N hours
    
    def validate(self) -> None:
        """Validate checkpoint configuration"""
        if not isinstance(self.enabled, bool):
            raise ValueError("Checkpoint enabled must be a boolean")
        if not isinstance(self.directory, Path):
            raise ValueError("Checkpoint directory must be a Path object")
        if not isinstance(self.cleanup_days, int) or self.cleanup_days < 1:
            raise ValueError("Cleanup days must be a positive integer")
        if not isinstance(self.auto_resume, bool):
            raise ValueError("Auto resume must be a boolean")
        if not isinstance(self.save_interval, int) or self.save_interval < 1:
            raise ValueError("Save interval must be a positive integer")

@dataclass
class MonitoringConfig:
    """Configuration for monitoring system"""
    enabled: bool = True
    metrics_dir: Path = Path('metrics')
    collection_interval: int = 60  # seconds
    alert_thresholds: Dict[str, float] = field(default_factory=lambda: {
        'memory_percent': 90.0,
        'cpu_percent': 90.0,
        'disk_percent': 90.0
    })
    retention_days: int = 30
    
    def validate(self) -> None:
        """Validate monitoring configuration"""
        if not isinstance(self.enabled, bool):
            raise ValueError("Monitoring enabled must be a boolean")
        if not isinstance(self.metrics_dir, Path):
            raise ValueError("Metrics directory must be a Path object")
        if not isinstance(self.collection_interval, int) or self.collection_interval < 1:
            raise ValueError("Collection interval must be a positive integer")
        if not isinstance(self.alert_thresholds, dict):
            raise ValueError("Alert thresholds must be a dictionary")
        for key in ['memory_percent', 'cpu_percent', 'disk_percent']:
            if key not in self.alert_thresholds:
                raise ValueError(f"Missing alert threshold for {key}")
            if not isinstance(self.alert_thresholds[key], (int, float)) or not (0 <= self.alert_thresholds[key] <= 100):
                raise ValueError(f"Alert threshold for {key} must be between 0 and 100")
        if not isinstance(self.retention_days, int) or self.retention_days < 1:
            raise ValueError("Retention days must be a positive integer")

class Config:
    def __init__(
        self,
        db_config: DatabaseConfig,
        instruments: Dict[str, InstrumentConfig],
        downloader: Optional[DownloaderConfig] = None,
        buffer: Optional[DatabaseBufferConfig] = None,
        logging: Optional[LoggingConfig] = None,
        validation: Optional[ValidationConfig] = None,
        checkpoint: Optional[CheckpointConfig] = None,
        monitoring: Optional[MonitoringConfig] = None
    ):
        self.db = db_config
        self.instruments = instruments
        self.downloader = downloader or DownloaderConfig()
        self.buffer = buffer or DatabaseBufferConfig()
        self.logging = logging or LoggingConfig()
        self.validation = validation or ValidationConfig()
        self.checkpoint = checkpoint or CheckpointConfig()
        self.monitoring = monitoring or MonitoringConfig()

    def validate(self) -> None:
        """Validate entire configuration"""
        self.db.validate()
        for instrument in self.instruments.values():
            instrument.validate()
        self.downloader.validate()
        self.buffer.validate()
        self.logging.validate()
        self.validation.validate()
        self.checkpoint.validate()
        self.monitoring.validate()

    def setup_logging(self) -> None:
        """Setup logging based on configuration"""
        self.logging.log_dir.mkdir(exist_ok=True)
        
        # Configure root logger
        logging.basicConfig(
            level=self.logging.level,
            format=self.logging.format
        )

        # Setup file handlers
        for name, handler_config in self.logging.file_handlers.items():
            handler = logging.FileHandler(
                self.logging.log_dir / handler_config['filename']
            )
            handler.setLevel(handler_config['level'])
            handler.setFormatter(logging.Formatter(self.logging.format))
            
            logger = logging.getLogger(name)
            logger.addHandler(handler)
            logger.setLevel(handler_config['level'])
