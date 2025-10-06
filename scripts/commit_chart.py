#!/usr/bin/env python3
# /// script
# dependencies = [
#   "pandas",
# ]
# ///

import subprocess
from collections import Counter
from datetime import datetime, timedelta

# Get commits from the past 4.5 days
days_ago = datetime.now() - timedelta(days=4.5)
since_date = days_ago.strftime("%Y-%m-%d %H:%M:%S")

# Get git log with timestamps
result = subprocess.run(
    ["git", "log", "--since", since_date, "--format=%cd", "--date=iso"],
    capture_output=True,
    text=True,
    cwd="/Users/nico/Code/sidemantic",
)

# Parse timestamps and group by hour
commits_by_hour = Counter()
for line in result.stdout.strip().split("\n"):
    if line:
        # Parse ISO format: 2024-01-15 14:23:45 -0800
        dt = datetime.strptime(line[:19], "%Y-%m-%d %H:%M:%S")
        hour_key = dt.strftime("%Y-%m-%d %H:00")
        commits_by_hour[hour_key] += 1

# Generate all hours in the range
start_hour = days_ago.replace(minute=0, second=0, microsecond=0)
end_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
all_hours = []
current = start_hour
while current <= end_hour:
    all_hours.append(current.strftime("%Y-%m-%d %H:00"))
    current += timedelta(hours=1)

# Find max count for scaling
max_count = max(commits_by_hour.values()) if commits_by_hour else 1
bar_width = 50

print("\nCommits by Hour (past 4.5 days)")
print(f"{'=' * 70}")

for hour in all_hours:
    count = commits_by_hour.get(hour, 0)
    # Create bar
    if count > 0:
        bar_length = int((count / max_count) * bar_width)
        bar = "█" * bar_length
        print(f"{hour}  {bar} {count}")
    else:
        print(f"{hour}  ")

total = sum(commits_by_hour.values())
print(f"{'=' * 70}")
print(f"Total commits: {total}\n")
