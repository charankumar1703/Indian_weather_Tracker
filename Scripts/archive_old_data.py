import sqlite3
from datetime import datetime

DB_PATH = r"C:\Indian_Weather_Tracker\data\weather_live.db"

def archive_old_data():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Archive data older than 90 days
    cursor.execute('''
        INSERT OR IGNORE INTO weather_archive
        SELECT *, ? FROM weather_live
        WHERE Timestamp < DATE('now', '-90 days')
    ''', (now_str,))

    # Delete those rows from weather_live
    cursor.execute('''
        DELETE FROM weather_live
        WHERE Timestamp < DATE('now', '-90 days')
    ''')

    conn.commit()
    conn.close()
    print(f"✅ Archived data older than 90 days. Onboarding_time = {now_str}")

if __name__ == "__main__":
    archive_old_data()
