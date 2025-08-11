Use Timetable Plugin (Airflow 2.2+)
A custom Timetable lets you define exactly which dates/times the DAG will run. You can completely remove holiday dates from the schedule.

Example:

from airflow.timetables.base import Timetable
from airflow.timetables.simple import CronDataIntervalTimetable
from pendulum import DateTime
import datetime

HOLIDAYS = [
    datetime.date(2025, 3, 14),  # Holi
    datetime.date(2025, 10, 20), # Diwali
    datetime.date(2025, 12, 25), # Christmas
]

class NoHolidayTimetable(Timetable):
    def __init__(self, base_cron: str):
        self.base_timetable = CronDataIntervalTimetable(base_cron, timezone="UTC")

    def next_dagrun_info(self, *, last_automated_data_interval, restriction):
        # Get next schedule from base cron
        next_info = self.base_timetable.next_dagrun_info(
            last_automated_data_interval=last_automated_data_interval,
            restriction=restriction
        )

        # Skip until we find a date that's not a holiday
        while next_info and next_info.start_date.date() in HOLIDAYS:
            next_info = self.base_timetable.next_dagrun_info(
                last_automated_data_interval=(next_info.start_date, next_info.end_date),
                restriction=restriction
            )

        return next_info


Then in your DAG:

from airflow import DAG
from datetime import datetime

with DAG(
    dag_id="no_holiday_dag",
    start_date=datetime(2025, 1, 1),
    timetable=NoHolidayTimetable("0 6 * * *"),  # run at 6 AM daily, except holidays
    catchup=False
) as dag:
    ...




