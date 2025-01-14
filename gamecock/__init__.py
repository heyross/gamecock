"""
Gamecock - SEC Filing Analysis and Processing Tool
"""

__version__ = "2.0.0"

"""Initialize logging configuration."""
from loguru import logger

# Remove default handler
logger.remove()

# Add custom handler for INFO and above only
logger.add(
    lambda msg: print(msg, end=""),
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}"
)
