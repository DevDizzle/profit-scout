import logging
import os

# Ensure log directory exists
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)

# Define log file path
LOG_FILE = os.path.join(LOG_DIR, "app.log")

# Configure logging format
logging.basicConfig(
    level=logging.DEBUG,  # Change to INFO or WARNING for production
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),  # Save logs to a file
        logging.StreamHandler()  # Print logs to console
    ]
)

# Create logger instance
logger = logging.getLogger("ProfitScout")

# Test log message
logger.info("✅ Logger initialized successfully")