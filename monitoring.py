"""
Monitoring and metrics system for Trading Data Downloader.
Tracks performance, data quality, and system health metrics.
"""

import time
from datetime import datetime, date
from typing import Dict, List, Optional, Set, Any
from dataclasses import dataclass, field
import logging
import asyncio
from collections import defaultdict
import psutil
import json
from pathlib import Path

@dataclass
class PerformanceMetrics:
    """Performance-related metrics"""
    download_time_ms: float = 0.0
    processing_time_ms: float = 0.0
    db_write_time_ms: float = 0.0
    total_time_ms: float = 0.0
    ticks_per_second: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_percent: float = 0.0

@dataclass
class DataQualityMetrics:
    """Data quality metrics"""
    total_ticks: int = 0
    valid_ticks: int = 0
    invalid_ticks: int = 0
    validation_errors: Dict[str, int] = field(default_factory=dict)
    missing_hours: int = 0
    data_gaps: List[tuple] = field(default_factory=list)

@dataclass
class SystemMetrics:
    """System health metrics"""
    process_memory_mb: float = 0.0
    system_memory_percent: float = 0.0
    cpu_percent: float = 0.0
    disk_usage_percent: float = 0.0
    open_files: int = 0
    thread_count: int = 0

class MetricsCollector:
    """Collects and manages various metrics"""
    
    def __init__(self, metrics_dir: Path):
        self.metrics_dir = metrics_dir
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        self.performance: Dict[str, Dict[date, List[PerformanceMetrics]]] = defaultdict(lambda: defaultdict(list))
        self.quality: Dict[str, Dict[date, DataQualityMetrics]] = defaultdict(lambda: defaultdict(DataQualityMetrics))
        self.system: List[SystemMetrics] = []
        self._start_time = time.time()
        self._process = psutil.Process()
        self._logger = logging.getLogger('metrics')

    async def collect_system_metrics(self):
        """Collect system metrics periodically"""
        while True:
            try:
                metrics = SystemMetrics(
                    process_memory_mb=self._process.memory_info().rss / 1024 / 1024,
                    system_memory_percent=psutil.virtual_memory().percent,
                    cpu_percent=psutil.cpu_percent(),
                    disk_usage_percent=psutil.disk_usage('/').percent,
                    open_files=len(self._process.open_files()),
                    thread_count=self._process.num_threads()
                )
                self.system.append(metrics)
                
                # Log alerts for high resource usage
                if metrics.system_memory_percent > 90:
                    self._logger.warning("High memory usage: %.1f%%", metrics.system_memory_percent)
                if metrics.cpu_percent > 90:
                    self._logger.warning("High CPU usage: %.1f%%", metrics.cpu_percent)
                if metrics.disk_usage_percent > 90:
                    self._logger.warning("High disk usage: %.1f%%", metrics.disk_usage_percent)
                
                await asyncio.sleep(60)  # Collect every minute
                
            except Exception as e:
                self._logger.error("Failed to collect system metrics: %s", str(e))
                await asyncio.sleep(5)  # Short delay on error

    def record_performance(
        self,
        instrument: str,
        day: date,
        download_time: float,
        processing_time: float,
        db_time: float,
        tick_count: int
    ):
        """Record performance metrics for an operation"""
        total_time = download_time + processing_time + db_time
        metrics = PerformanceMetrics(
            download_time_ms=download_time * 1000,
            processing_time_ms=processing_time * 1000,
            db_write_time_ms=db_time * 1000,
            total_time_ms=total_time * 1000,
            ticks_per_second=tick_count / total_time if total_time > 0 else 0,
            memory_usage_mb=self._process.memory_info().rss / 1024 / 1024,
            cpu_percent=self._process.cpu_percent()
        )
        self.performance[instrument][day].append(metrics)

    def update_quality_metrics(
        self,
        instrument: str,
        day: date,
        total_ticks: int,
        valid_ticks: int,
        validation_errors: Dict[str, int],
        missing_hours: int,
        gaps: List[tuple]
    ):
        """Update data quality metrics"""
        metrics = self.quality[instrument][day]
        metrics.total_ticks += total_ticks
        metrics.valid_ticks += valid_ticks
        metrics.invalid_ticks += (total_ticks - valid_ticks)
        
        # Update validation error counts
        for error_type, count in validation_errors.items():
            metrics.validation_errors[error_type] = (
                metrics.validation_errors.get(error_type, 0) + count
            )
        
        metrics.missing_hours = missing_hours
        metrics.data_gaps.extend(gaps)

    def get_summary(self, instrument: str, day: Optional[date] = None) -> dict:
        """Get summary of metrics for an instrument"""
        summary = {
            'performance': {
                'avg_download_time_ms': 0.0,
                'avg_processing_time_ms': 0.0,
                'avg_db_write_time_ms': 0.0,
                'avg_ticks_per_second': 0.0,
                'peak_memory_usage_mb': 0.0
            },
            'quality': {
                'total_ticks': 0,
                'valid_ticks': 0,
                'invalid_ticks': 0,
                'validation_errors': {},
                'missing_hours': 0,
                'data_gaps': 0
            },
            'system': {
                'avg_memory_usage_percent': 0.0,
                'avg_cpu_percent': 0.0,
                'peak_memory_percent': 0.0,
                'peak_cpu_percent': 0.0
            }
        }
        
        # Performance metrics
        perf_metrics = (
            self.performance[instrument][day] if day
            else [m for d in self.performance[instrument].values() for m in d]
        )
        if perf_metrics:
            summary['performance'].update({
                'avg_download_time_ms': sum(m.download_time_ms for m in perf_metrics) / len(perf_metrics),
                'avg_processing_time_ms': sum(m.processing_time_ms for m in perf_metrics) / len(perf_metrics),
                'avg_db_write_time_ms': sum(m.db_write_time_ms for m in perf_metrics) / len(perf_metrics),
                'avg_ticks_per_second': sum(m.ticks_per_second for m in perf_metrics) / len(perf_metrics),
                'peak_memory_usage_mb': max(m.memory_usage_mb for m in perf_metrics)
            })
        
        # Quality metrics
        quality_metrics = (
            [self.quality[instrument][day]] if day
            else self.quality[instrument].values()
        )
        for metrics in quality_metrics:
            summary['quality']['total_ticks'] += metrics.total_ticks
            summary['quality']['valid_ticks'] += metrics.valid_ticks
            summary['quality']['invalid_ticks'] += metrics.invalid_ticks
            summary['quality']['missing_hours'] += metrics.missing_hours
            summary['quality']['data_gaps'] += len(metrics.data_gaps)
            for error_type, count in metrics.validation_errors.items():
                summary['quality']['validation_errors'][error_type] = (
                    summary['quality']['validation_errors'].get(error_type, 0) + count
                )
        
        # System metrics
        if self.system:
            summary['system'].update({
                'avg_memory_usage_percent': sum(m.system_memory_percent for m in self.system) / len(self.system),
                'avg_cpu_percent': sum(m.cpu_percent for m in self.system) / len(self.system),
                'peak_memory_percent': max(m.system_memory_percent for m in self.system),
                'peak_cpu_percent': max(m.cpu_percent for m in self.system)
            })
        
        return summary

    def save_metrics(self):
        """Save metrics to files"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save performance metrics
        perf_file = self.metrics_dir / f'performance_{timestamp}.json'
        perf_data = {
            instrument: {
                str(d): [vars(m) for m in metrics]
                for d, metrics in days.items()
            }
            for instrument, days in self.performance.items()
        }
        perf_file.write_text(json.dumps(perf_data, indent=2))
        
        # Save quality metrics
        quality_file = self.metrics_dir / f'quality_{timestamp}.json'
        quality_data = {
            instrument: {
                str(d): vars(metrics)
                for d, metrics in days.items()
            }
            for instrument, days in self.quality.items()
        }
        quality_file.write_text(json.dumps(quality_data, indent=2))
        
        # Save system metrics
        system_file = self.metrics_dir / f'system_{timestamp}.json'
        system_data = [vars(m) for m in self.system]
        system_file.write_text(json.dumps(system_data, indent=2))

    def cleanup_old_metrics(self, days_to_keep: int = 30):
        """Remove old metric files"""
        cutoff = time.time() - (days_to_keep * 24 * 60 * 60)
        for file in self.metrics_dir.glob('*.json'):
            if file.stat().st_mtime < cutoff:
                file.unlink()

class MetricsManager:
    """Manages metrics collection and monitoring"""
    
    def __init__(self, config_dir: Path):
        self.metrics_dir = config_dir / 'metrics'
        self.collector = MetricsCollector(self.metrics_dir)
        self._monitoring_task: Optional[asyncio.Task] = None
        self._logger = logging.getLogger('monitoring')

    async def start_monitoring(self):
        """Start monitoring system metrics"""
        self._monitoring_task = asyncio.create_task(
            self.collector.collect_system_metrics()
        )
        self._logger.info("Started system monitoring")

    async def stop_monitoring(self):
        """Stop monitoring and save metrics"""
        if self._monitoring_task:
            self._monitoring_task.cancel()
            try:
                await self._monitoring_task
            except asyncio.CancelledError:
                pass
            
        self.collector.save_metrics()
        self.collector.cleanup_old_metrics()
        self._logger.info("Stopped monitoring and saved metrics")

    def get_metrics_summary(self) -> dict:
        """Get summary of all metrics"""
        summary = {
            'instruments': {},
            'system': {
                'uptime_hours': (time.time() - self.collector._start_time) / 3600,
                'current_memory_mb': self.collector._process.memory_info().rss / 1024 / 1024,
                'current_cpu_percent': self.collector._process.cpu_percent(),
                'total_open_files': len(self.collector._process.open_files()),
                'thread_count': self.collector._process.num_threads()
            }
        }
        
        # Get summaries for each instrument
        for instrument in self.collector.performance.keys():
            summary['instruments'][instrument] = self.collector.get_summary(instrument)
        
        return summary
