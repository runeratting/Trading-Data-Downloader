"""
Optimized data processing module for Trading Data Downloader.
Uses numpy for efficient tick data processing and transformation.
"""

import numpy as np
from datetime import datetime, timezone
from typing import Tuple, List, Optional
import struct
from dataclasses import dataclass

@dataclass
class TickBatch:
    """Container for batch-processed tick data"""
    timestamps: np.ndarray  # Unix timestamps in milliseconds
    bids: np.ndarray  # Bid prices
    asks: np.ndarray  # Ask prices
    bid_volumes: np.ndarray  # Bid volumes
    ask_volumes: np.ndarray  # Ask volumes
    
    @property
    def size(self) -> int:
        """Number of ticks in the batch"""
        return len(self.timestamps)
    
    def to_tuples(self) -> List[Tuple]:
        """Convert batch to list of tuples for compatibility"""
        return list(zip(
            self.timestamps.tolist(),
            self.bids.tolist(),
            self.asks.tolist(),
            self.bid_volumes.tolist(),
            self.ask_volumes.tolist()
        ))
    
    @classmethod
    def from_tuples(cls, ticks: List[Tuple]) -> 'TickBatch':
        """Create batch from list of tuples"""
        if not ticks:
            return cls(
                timestamps=np.array([], dtype=np.int64),
                bids=np.array([], dtype=np.float64),
                asks=np.array([], dtype=np.float64),
                bid_volumes=np.array([], dtype=np.float64),
                ask_volumes=np.array([], dtype=np.float64)
            )
            
        arrays = list(map(np.array, zip(*ticks)))
        return cls(
            timestamps=arrays[0].astype(np.int64),
            bids=arrays[1].astype(np.float64),
            asks=arrays[2].astype(np.float64),
            bid_volumes=arrays[3].astype(np.float64),
            ask_volumes=arrays[4].astype(np.float64)
        )

class TickProcessor:
    """High-performance tick data processor using numpy"""
    
    TICK_SIZE = 20  # Size of each tick record in bytes (4 + 4 + 4 + 4 + 4 = 20)
    TICK_STRUCT = struct.Struct('!IIIff')  # Struct for unpacking tick data (I=uint32, f=float32)
    
    @staticmethod
    def decode_ticks(
        raw_data: bytes,
        base_timestamp: int,
        point_value: int,
        chunk_size: int = 10000
    ) -> TickBatch:
        """
        Decode binary tick data using numpy for performance
        
        Args:
            raw_data: Raw binary tick data
            base_timestamp: Base timestamp for the hour
            point_value: Point value for price conversion
            chunk_size: Number of ticks to process in each chunk
            
        Returns:
            TickBatch containing decoded tick data
        """
        # Calculate number of complete ticks
        n_ticks = len(raw_data) // TickProcessor.TICK_SIZE
        if n_ticks == 0:
            return TickBatch(
                timestamps=np.array([], dtype=np.int64),
                bids=np.array([], dtype=np.float64),
                asks=np.array([], dtype=np.float64),
                bid_volumes=np.array([], dtype=np.float64),
                ask_volumes=np.array([], dtype=np.float64)
            )
        
        # Pre-allocate arrays
        timestamps = np.empty(n_ticks, dtype=np.int64)
        bids = np.empty(n_ticks, dtype=np.float64)
        asks = np.empty(n_ticks, dtype=np.float64)
        bid_volumes = np.empty(n_ticks, dtype=np.float64)
        ask_volumes = np.empty(n_ticks, dtype=np.float64)
        
        # Process in chunks for better memory usage
        for i in range(0, n_ticks, chunk_size):
            end_idx = min(i + chunk_size, n_ticks)
            chunk_size_actual = end_idx - i
            
            # Extract chunk of raw data
            chunk_start = i * TickProcessor.TICK_SIZE
            chunk_end = chunk_start + (chunk_size_actual * TickProcessor.TICK_SIZE)
            chunk = raw_data[chunk_start:chunk_end]
            
            # Unpack chunk into temporary arrays
            chunk_data = np.frombuffer(
                chunk,
                dtype=np.dtype([
                    ('ms_offset', '>u4'),
                    ('bid_raw', '>u4'),
                    ('ask_raw', '>u4'),
                    ('vol_bid', '>f4'),
                    ('vol_ask', '>f4')
                ])
            )
            
            # Convert and store in final arrays
            # Convert base_timestamp to int64 to prevent overflow
            timestamps[i:end_idx] = np.int64(base_timestamp) + chunk_data['ms_offset'].astype(np.int64)
            bids[i:end_idx] = chunk_data['bid_raw'].astype(np.float64) / point_value
            asks[i:end_idx] = chunk_data['ask_raw'].astype(np.float64) / point_value
            bid_volumes[i:end_idx] = np.round(chunk_data['vol_bid'] * 1_000_000)
            ask_volumes[i:end_idx] = np.round(chunk_data['vol_ask'] * 1_000_000)
        
        return TickBatch(timestamps, bids, asks, bid_volumes, ask_volumes)
    
    @staticmethod
    def calculate_statistics(batch: TickBatch) -> dict:
        """
        Calculate statistics for a batch of ticks
        
        Args:
            batch: TickBatch to analyze
            
        Returns:
            Dictionary of statistics
        """
        if batch.size == 0:
            return {
                'count': 0,
                'price_stats': None,
                'volume_stats': None,
                'timestamp_stats': None
            }
        
        # Price statistics
        mid_prices = (batch.bids + batch.asks) / 2
        spreads = batch.asks - batch.bids
        price_stats = {
            'min_bid': float(np.min(batch.bids)),
            'max_bid': float(np.max(batch.bids)),
            'avg_bid': float(np.mean(batch.bids)),
            'min_ask': float(np.min(batch.asks)),
            'max_ask': float(np.max(batch.asks)),
            'avg_ask': float(np.mean(batch.asks)),
            'min_spread': float(np.min(spreads)),
            'max_spread': float(np.max(spreads)),
            'avg_spread': float(np.mean(spreads)),
            'price_volatility': float(np.std(mid_prices))
        }
        
        # Volume statistics
        volume_stats = {
            'total_bid_volume': float(np.sum(batch.bid_volumes)),
            'total_ask_volume': float(np.sum(batch.ask_volumes)),
            'avg_bid_volume': float(np.mean(batch.bid_volumes)),
            'avg_ask_volume': float(np.mean(batch.ask_volumes)),
            'max_bid_volume': float(np.max(batch.bid_volumes)),
            'max_ask_volume': float(np.max(batch.ask_volumes))
        }
        
        # Timestamp statistics
        timestamp_diffs = np.diff(batch.timestamps)
        timestamp_stats = {
            'min_interval': float(np.min(timestamp_diffs)) / 1000,  # Convert to seconds
            'max_interval': float(np.max(timestamp_diffs)) / 1000,
            'avg_interval': float(np.mean(timestamp_diffs)) / 1000,
            'tick_rate': float(len(batch.timestamps) / (
                (batch.timestamps[-1] - batch.timestamps[0]) / 1000
            ))  # Ticks per second
        }
        
        return {
            'count': batch.size,
            'price_stats': price_stats,
            'volume_stats': volume_stats,
            'timestamp_stats': timestamp_stats
        }
    
    @staticmethod
    def find_anomalies(batch: TickBatch, config: dict) -> dict:
        """
        Find anomalies in tick data
        
        Args:
            batch: TickBatch to analyze
            config: Dictionary of threshold configurations
            
        Returns:
            Dictionary of anomaly counts and details
        """
        if batch.size == 0:
            return {'total_anomalies': 0, 'anomalies': {}}
        
        anomalies = {
            'price_anomalies': [],
            'volume_anomalies': [],
            'timestamp_anomalies': []
        }
        
        # Price anomalies
        spreads = batch.asks - batch.bids
        spread_pct = spreads / ((batch.asks + batch.bids) / 2)
        price_jumps = np.abs(np.diff(batch.bids)) / batch.bids[:-1]
        
        # Find large spreads
        large_spread_mask = spread_pct > config.get('max_spread_pct', 0.01)
        if np.any(large_spread_mask):
            anomalies['price_anomalies'].extend([
                {
                    'type': 'large_spread',
                    'timestamp': int(ts),
                    'spread_pct': float(pct)
                }
                for ts, pct in zip(
                    batch.timestamps[large_spread_mask],
                    spread_pct[large_spread_mask]
                )
            ])
        
        # Find price jumps
        jump_mask = price_jumps > config.get('max_price_jump_pct', 0.005)
        if np.any(jump_mask):
            anomalies['price_anomalies'].extend([
                {
                    'type': 'price_jump',
                    'timestamp': int(ts),
                    'jump_pct': float(jmp)
                }
                for ts, jmp in zip(
                    batch.timestamps[1:][jump_mask],
                    price_jumps[jump_mask]
                )
            ])
        
        # Volume anomalies
        large_volume_mask = (
            (batch.bid_volumes > config.get('max_volume', 1_000_000)) |
            (batch.ask_volumes > config.get('max_volume', 1_000_000))
        )
        if np.any(large_volume_mask):
            anomalies['volume_anomalies'].extend([
                {
                    'type': 'large_volume',
                    'timestamp': int(ts),
                    'bid_volume': float(bv),
                    'ask_volume': float(av)
                }
                for ts, bv, av in zip(
                    batch.timestamps[large_volume_mask],
                    batch.bid_volumes[large_volume_mask],
                    batch.ask_volumes[large_volume_mask]
                )
            ])
        
        # Timestamp anomalies
        timestamp_diffs = np.diff(batch.timestamps)
        gap_mask = timestamp_diffs > config.get('max_tick_interval', 1000)
        if np.any(gap_mask):
            anomalies['timestamp_anomalies'].extend([
                {
                    'type': 'time_gap',
                    'start_timestamp': int(ts1),
                    'end_timestamp': int(ts2),
                    'gap_seconds': float(gap) / 1000
                }
                for ts1, ts2, gap in zip(
                    batch.timestamps[:-1][gap_mask],
                    batch.timestamps[1:][gap_mask],
                    timestamp_diffs[gap_mask]
                )
            ])
        
        total_anomalies = sum(len(v) for v in anomalies.values())
        return {
            'total_anomalies': total_anomalies,
            'anomalies': anomalies
        }
