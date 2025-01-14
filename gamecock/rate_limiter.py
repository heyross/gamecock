"""Rate limiter implementation using token bucket algorithm."""
import time
from threading import Lock
from loguru import logger

class RateLimiter:
    """Token bucket rate limiter."""
    
    def __init__(self, max_requests: int = 9, time_window: float = 1.0):
        """Initialize rate limiter.
        
        Args:
            max_requests: Maximum number of requests allowed per time window
            time_window: Time window in seconds
        """
        self.max_tokens = max_requests
        self.tokens = max_requests
        self.time_window = time_window
        self.last_update = time.time()
        self.lock = Lock()
        
    def _add_tokens(self):
        """Add tokens based on elapsed time."""
        now = time.time()
        time_passed = now - self.last_update
        new_tokens = time_passed * (self.max_tokens / self.time_window)
        self.tokens = min(self.max_tokens, self.tokens + new_tokens)
        self.last_update = now
        
    def acquire(self):
        """Acquire a token, blocking if necessary."""
        with self.lock:
            while True:
                self._add_tokens()
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                sleep_time = (1 - self.tokens) * (self.time_window / self.max_tokens)
                logger.debug(f"Rate limit reached, sleeping for {sleep_time:.2f}s")
                time.sleep(sleep_time)
