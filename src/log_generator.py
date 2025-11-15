import time
import random
from datetime import datetime
import os

LOG_LEVELS = ["INFO", "ERROR", "WARN", "DEBUG"]

def generate_log():
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    level = random.choice(LOG_LEVELS)
    message = f"This is a simulated {level} log message."
    return f"[{timestamp}] [{level}] {message}"

def write_logs(log_dir, interval=2):
    os.makedirs(log_dir, exist_ok=True)

    while True:
        log_message = generate_log()
        filename = f"log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
        filepath = os.path.join(log_dir, filename)
        with open(filepath, "w") as f:
            f.write(log_message)
        print(f"Generated log: {log_message}")
        time.sleep(interval)

if __name__ == "__main__":
    log_directory = "./logs"
    write_logs(log_directory)
