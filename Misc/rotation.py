import calendar
from datetime import datetime, timedelta
import pandas as pd

def find_first_monday(year, month):
    """Finds the first Monday of the given month and year."""
    # Month's first day is always 1
    day = 1
    # Find out what day of the week the first day of the month is
    # 0 is Monday, 1 is Tuesday, ..., 6 is Sunday
    first_day_of_month = datetime(year, month, day)
    first_day_weekday = first_day_of_month.weekday()
    # Calculate how many days to add to get to the first Monday
    # If the first day is already Monday (0), we don't need to add any days
    days_to_add = (7 - first_day_weekday) % 7
    first_monday = first_day_of_month + timedelta(days=days_to_add)
    return first_monday

# Current year and month
current_year = datetime.now().year
current_month = datetime.now().month

# Find the first Monday of the current month
start_date = find_first_monday(current_year, current_month)

# List of team members
team_members = ['P1', 'P2', 'P3', 'P4', 'P5', 'P6']
# Days to cover
days = ['Monday', 'Wednesday', 'Thursday', 'Friday']

# Generate schedule with backups
schedule = []
for week in range(52):  # Example: 4 weeks
    for day in range(len(days)):
        person_index = (week + day) % len(team_members)
        backup_index = (person_index + 1) % len(team_members)  # Next person in line as backup
        date = start_date + timedelta(days=week*7) + timedelta(days=day)
        schedule.append({
            'Date': date.strftime('%Y-%m-%d'),
            'Day': days[day % len(days)],
            'Person': team_members[person_index],
            'Backup': team_members[backup_index]
        })

# Convert to DataFrame for easy viewing/manipulation
df_schedule = pd.DataFrame(schedule)
print(df_schedule)

# Save to CSV
df_schedule.to_csv('rotation_schedule.csv', index=False)
print("Schedule saved to rotation_schedule.csv")
