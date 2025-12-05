import os
import sys
import time
import shutil
import random

# No need for Spark/Java config in simulator
BASE_DIR = r"d:\app\Downloads\NetPlag\data"
SOURCE = os.path.join(BASE_DIR, "stream_source")
DEST = os.path.join(BASE_DIR, "stream_input")

print("--- SIMULATEUR DE FLUX ---")
print(f"Source: {SOURCE}")
print(f"Destination: {DEST}")

# Create destination folder if it doesn't exist
os.makedirs(DEST, exist_ok=True)

# Get all .txt files
files = [f for f in os.listdir(SOURCE) if f.endswith(".txt")]
total_files = len(files)

print(f"\nFound {total_files} files to send")
print("Starting simulation...\n")

for idx, f in enumerate(files, 1):
    src_path = os.path.join(SOURCE, f)
    dst_path = os.path.join(DEST, f)
    
    shutil.copy(src_path, dst_path)
    print(f"[{idx}/{total_files}] Sent: {f}")
    
    # Wait 1-3 seconds between files
    time.sleep(random.randint(1, 3))

print(f"\n✓ Simulation complete - {total_files} files sent")