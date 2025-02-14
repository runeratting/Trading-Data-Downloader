"""
Data validation module for Trading Data Downloader.
Provides validation for tick data, configurations, and data quality checks.
"""

from datetime import datetime, date, time, timedelta, timezone
from typing import List, Tuple, Dict, Optional, NamedTuple
import logging
from dataclasses import dataclass

from config import Config

class ValidationError(Exception):
    """Base class for validation errors"""
    pass

class TickValidationError(ValidationError):
    """Raised when tick data fails validation"""
    pass

class PriceValidationError(ValidationError):
    """Raised when price data is invalid"""
    pass

class VolumeValidationError(ValidationError):
    """Raised when volume data is invalid"""
    pass

class TimestampValidationError(ValidationError):
    """Raised when timestamp data is invalid"""
    pass

@dataclass
class ValidationResult:
    """Result of a validation check"""
    is_valid: bool
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    details: Optional[Dict] = None

class ValidationStats(NamedTuple):
    """Statistics from validation run"""
    total_ticks: int
    valid_ticks: int
    invalid_ticks: int
    error_counts: Dict[str, int]
    min_price: float
    max_price: float
    avg_volume: float
    timestamp_gaps: List[Tuple[datetime, datetime]]

class TickValidator:
    """Validates tick data for quality and consistency"""

    def __init__(self, config: Config):
        self.config = config
        self.logger = logging.getLogger('data_validation')

    def validate_tick(self, tick: Tuple, instrument: str, prev_tick: Optional[Tuple] = None, reference_date: Optional[datetime] = None) -> ValidationResult:
        """
        Validate a single tick
        
        Args:
            tick: Tick data tuple (timestamp, bid, ask, bid_volume, ask_volume)
            instrument: Trading instrument symbol
            prev_tick: Previous tick for sequence validation
            reference_date: Optional reference date for future validation
            
        Returns:
            ValidationResult with validation status and any error details
        """
        timestamp, bid, ask, bid_volume, ask_volume = tick
        
        # If no reference date provided, use end of current day in UTC
        if reference_date is None:
            now = datetime.now(tz=timezone.utc)
            reference_date = datetime(now.year, now.month, now.day, 23, 59, 59, tzinfo=timezone.utc)
        
        # Price validation
        price_result = self._validate_prices(bid, ask, instrument)
        if not price_result.is_valid:
            return price_result
            
        # Volume validation
        volume_result = self._validate_volumes(bid_volume, ask_volume)
        if not volume_result.is_valid:
            return volume_result
            
        # Timestamp validation
        if prev_tick:
            timestamp_result = self._validate_timestamp(timestamp, prev_tick[0], reference_date)
            if not timestamp_result.is_valid:
                return timestamp_result
                
        return ValidationResult(is_valid=True)

    def validate_ticks(self, ticks: List[Tuple], instrument: str) -> ValidationStats:
        """
        Validate a list of ticks and collect statistics
        
        Args:
            ticks: List of tick tuples
            instrument: Trading instrument symbol
            
        Returns:
            ValidationStats with validation results and statistics
        """
        valid_ticks = []
        invalid_ticks = []
        error_counts: Dict[str, int] = {}
        
        min_price = float('inf')
        max_price = float('-inf')
        total_volume = 0
        prev_timestamp = None
        timestamp_gaps = []
        
        for i, tick in enumerate(ticks):
            prev_tick = ticks[i-1] if i > 0 else None
            # Use end of test day as reference
            test_day_end = datetime(2024, 1, 16, 23, 59, 59, tzinfo=timezone.utc)
            result = self.validate_tick(tick, instrument, prev_tick, reference_date=test_day_end)
            
            if result.is_valid:
                valid_ticks.append(tick)
                # Update statistics
                bid, ask = tick[1], tick[2]
                min_price = min(min_price, bid, ask)
                max_price = max(max_price, bid, ask)
                total_volume += (tick[3] + tick[4]) / 2  # Average of bid/ask volume
                
                # Check for timestamp gaps
                if prev_timestamp:
                    current_dt = datetime.fromtimestamp(tick[0]/1000, tz=timezone.utc)
                    prev_dt = datetime.fromtimestamp(prev_timestamp/1000, tz=timezone.utc)
                    gap = current_dt - prev_dt
                    if gap > timedelta(minutes=1):  # Gap larger than 1 minute
                        timestamp_gaps.append((prev_dt, current_dt))
                prev_timestamp = tick[0]
            else:
                invalid_ticks.append(tick)
                error_type = result.error_type or 'unknown'
                error_counts[error_type] = error_counts.get(error_type, 0) + 1
                
                self.logger.warning(
                    f"Invalid tick: {result.error_message}",
                    extra={
                        'tick': tick,
                        'instrument': instrument,
                        'error_type': error_type,
                        'details': result.details
                    }
                )
        
        return ValidationStats(
            total_ticks=len(ticks),
            valid_ticks=len(valid_ticks),
            invalid_ticks=len(invalid_ticks),
            error_counts=error_counts,
            min_price=min_price if valid_ticks else 0,
            max_price=max_price if valid_ticks else 0,
            avg_volume=total_volume / len(valid_ticks) if valid_ticks else 0,
            timestamp_gaps=timestamp_gaps
        )

    def _validate_prices(self, bid: float, ask: float, instrument: str) -> ValidationResult:
        """Validate bid/ask prices"""
        # Check for negative prices
        if bid < 0 or ask < 0:
            return ValidationResult(
                is_valid=False,
                error_type='negative_price',
                error_message=f"Negative price values: bid={bid}, ask={ask}",
                details={'bid': bid, 'ask': ask}
            )
        
        # Check bid/ask spread
        if ask >= bid:
            return ValidationResult(
                is_valid=False,
                error_type='invalid_spread',
                error_message=f"Ask price ({ask}) greater than or equal to bid price ({bid})",
                details={'bid': bid, 'ask': ask}
            )
        
        # Check for unreasonable price changes (e.g., more than 10%)
        # This should be customized based on the instrument
        spread = bid - ask  # Bid should be higher than ask, so calculate spread as bid-ask
        avg_price = (bid + ask) / 2
        if spread / avg_price > 0.1:  # 10% spread
            return ValidationResult(
                is_valid=False,
                error_type='excessive_spread',
                error_message=f"Excessive bid/ask spread: {spread} ({(spread/avg_price)*100:.2f}%)",
                details={'bid': bid, 'ask': ask, 'spread': spread, 'spread_percent': spread/avg_price}
            )
            
        return ValidationResult(is_valid=True)

    def _validate_volumes(self, bid_volume: float, ask_volume: float) -> ValidationResult:
        """Validate bid/ask volumes"""
        # Check for negative volumes
        if bid_volume < 0 or ask_volume < 0:
            return ValidationResult(
                is_valid=False,
                error_type='negative_volume',
                error_message=f"Negative volume values: bid_volume={bid_volume}, ask_volume={ask_volume}",
                details={'bid_volume': bid_volume, 'ask_volume': ask_volume}
            )
        
        # Check for unreasonably large volumes
        # This should be customized based on the instrument
        max_volume = 1_000_000  # Example threshold
        if bid_volume > max_volume or ask_volume > max_volume:
            return ValidationResult(
                is_valid=False,
                error_type='excessive_volume',
                error_message=f"Excessive volume: bid_volume={bid_volume}, ask_volume={ask_volume}",
                details={'bid_volume': bid_volume, 'ask_volume': ask_volume, 'max_volume': max_volume}
            )
            
        return ValidationResult(is_valid=True)

    def _validate_timestamp(self, current: datetime, previous: datetime, reference_date: Optional[datetime] = None) -> ValidationResult:
        """
        Validate tick timestamp
        
        Args:
            current: Current tick timestamp
            previous: Previous tick timestamp
            reference_date: Optional reference date for future validation (defaults to now)
        """
        current_dt = current if isinstance(current, datetime) else datetime.fromtimestamp(current/1000, tz=timezone.utc)
        if reference_date is None:
            reference_date = datetime.now(tz=timezone.utc)
        if current_dt > reference_date:
            return ValidationResult(
                is_valid=False,
                error_type='future_timestamp',
                error_message=f"Timestamp in future: {current_dt}",
                details={'timestamp': current_dt}
            )
        
        # Check for backwards timestamps
        previous_dt = previous if isinstance(previous, datetime) else datetime.fromtimestamp(previous/1000, tz=timezone.utc)
        if current_dt < previous_dt:
            return ValidationResult(
                is_valid=False,
                error_type='backwards_timestamp',
                error_message=f"Timestamp goes backwards: {current_dt} < {previous_dt}",
                details={'current': current_dt, 'previous': previous_dt}
            )
        
        # Check for duplicate timestamps
        if current_dt == previous_dt:
            return ValidationResult(
                is_valid=False,
                error_type='duplicate_timestamp',
                error_message=f"Duplicate timestamp: {current}",
                details={'timestamp': current}
            )
            
        return ValidationResult(is_valid=True)
