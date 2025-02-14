"""
Checkpoint and recovery system for Trading Data Downloader.
Tracks download progress and enables resuming from failures.
"""

import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Set
from dataclasses import dataclass, asdict
import logging
import asyncio
from contextlib import asynccontextmanager

@dataclass
class DownloadState:
    """State of a download operation"""
    instrument: str
    date: date
    completed_hours: Set[int]
    failed_hours: Dict[int, str]  # hour -> error message
    total_ticks: int
    last_updated: datetime
    
    @classmethod
    def from_dict(cls, data: dict) -> 'DownloadState':
        """Create state from dictionary"""
        return cls(
            instrument=data['instrument'],
            date=datetime.strptime(data['date'], '%Y-%m-%d').date(),
            completed_hours=set(data['completed_hours']),
            failed_hours={int(h): msg for h, msg in data['failed_hours'].items()},
            total_ticks=data['total_ticks'],
            last_updated=datetime.fromisoformat(data['last_updated'])
        )
    
    def to_dict(self) -> dict:
        """Convert state to dictionary"""
        return {
            'instrument': self.instrument,
            'date': self.date.strftime('%Y-%m-%d'),
            'completed_hours': list(self.completed_hours),
            'failed_hours': {str(h): msg for h, msg in self.failed_hours.items()},
            'total_ticks': self.total_ticks,
            'last_updated': self.last_updated.isoformat()
        }

class CheckpointManager:
    """Manages download checkpoints and recovery"""
    
    def __init__(self, checkpoint_dir: Path):
        self.checkpoint_dir = checkpoint_dir
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.active_states: Dict[str, Dict[date, DownloadState]] = {}
        self._lock = asyncio.Lock()
    
    def _get_checkpoint_path(self, instrument: str, day: date) -> Path:
        """Get path for checkpoint file"""
        return self.checkpoint_dir / f"{instrument}_{day.strftime('%Y%m%d')}.json"
    
    async def load_state(self, instrument: str, day: date) -> Optional[DownloadState]:
        """Load download state from checkpoint"""
        path = self._get_checkpoint_path(instrument, day)
        try:
            if path.exists():
                async with self._lock:
                    data = json.loads(path.read_text())
                    state = DownloadState.from_dict(data)
                    if instrument not in self.active_states:
                        self.active_states[instrument] = {}
                    self.active_states[instrument][day] = state
                    return state
        except Exception as e:
            logging.error(f"Failed to load checkpoint for {instrument} on {day}: {str(e)}")
        return None
    
    async def save_state(self, state: DownloadState):
        """Save download state to checkpoint"""
        path = self._get_checkpoint_path(state.instrument, state.date)
        try:
            async with self._lock:
                if state.instrument not in self.active_states:
                    self.active_states[state.instrument] = {}
                self.active_states[state.instrument][state.date] = state
                path.write_text(json.dumps(state.to_dict(), indent=2))
        except Exception as e:
            logging.error(
                f"Failed to save checkpoint for {state.instrument} on {state.date}: {str(e)}"
            )
    
    async def mark_hour_complete(
        self,
        instrument: str,
        day: date,
        hour: int,
        tick_count: int
    ):
        """Mark an hour as successfully completed"""
        async with self._lock:
            state = self.active_states.get(instrument, {}).get(day)
            if not state:
                state = DownloadState(
                    instrument=instrument,
                    date=day,
                    completed_hours=set(),
                    failed_hours={},
                    total_ticks=0,
                    last_updated=datetime.now()
                )
            
            state.completed_hours.add(hour)
            if hour in state.failed_hours:
                del state.failed_hours[hour]
            state.total_ticks += tick_count
            state.last_updated = datetime.now()
            
            await self.save_state(state)
    
    async def mark_hour_failed(
        self,
        instrument: str,
        day: date,
        hour: int,
        error: str
    ):
        """Mark an hour as failed with error message"""
        async with self._lock:
            state = self.active_states.get(instrument, {}).get(day)
            if not state:
                state = DownloadState(
                    instrument=instrument,
                    date=day,
                    completed_hours=set(),
                    failed_hours={},
                    total_ticks=0,
                    last_updated=datetime.now()
                )
            
            state.failed_hours[hour] = error
            if hour in state.completed_hours:
                state.completed_hours.remove(hour)
            state.last_updated = datetime.now()
            
            await self.save_state(state)
    
    def get_incomplete_hours(
        self,
        instrument: str,
        day: date,
        trading_hours: Set[int]
    ) -> Set[int]:
        """Get set of trading hours that need to be downloaded"""
        state = self.active_states.get(instrument, {}).get(day)
        if not state:
            return trading_hours
        return trading_hours - state.completed_hours
    
    def get_failed_hours(
        self,
        instrument: str,
        day: date
    ) -> Dict[int, str]:
        """Get hours that failed with their error messages"""
        state = self.active_states.get(instrument, {}).get(day)
        if not state:
            return {}
        return state.failed_hours.copy()
    
    @asynccontextmanager
    async def track_progress(
        self,
        instrument: str,
        day: date,
        hour: int
    ):
        """Context manager for tracking download progress"""
        try:
            yield
        except Exception as e:
            await self.mark_hour_failed(instrument, day, hour, str(e))
            raise
    
    async def cleanup_old_checkpoints(self, days_to_keep: int = 30):
        """Remove checkpoint files older than specified days"""
        cutoff = datetime.now() - timedelta(days=days_to_keep)
        async with self._lock:
            for path in self.checkpoint_dir.glob("*.json"):
                try:
                    # Extract date from filename
                    date_str = path.stem.split('_')[1]
                    checkpoint_date = datetime.strptime(date_str, '%Y%m%d')
                    if checkpoint_date < cutoff:
                        path.unlink()
                except Exception as e:
                    logging.error(f"Failed to cleanup checkpoint {path}: {str(e)}")
    
    def get_download_summary(self) -> Dict[str, Dict[str, any]]:
        """Get summary of download progress"""
        summary = {}
        for instrument, days in self.active_states.items():
            instrument_summary = {
                'total_days': len(days),
                'completed_days': sum(
                    1 for state in days.values()
                    if not state.failed_hours
                ),
                'total_ticks': sum(
                    state.total_ticks for state in days.values()
                ),
                'failed_hours': sum(
                    len(state.failed_hours) for state in days.values()
                ),
                'last_updated': max(
                    state.last_updated for state in days.values()
                ).isoformat() if days else None
            }
            summary[instrument] = instrument_summary
        return summary
