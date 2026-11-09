"""spark-submit's entry point for the station counts job, bikes.station_counts.

    spark-submit jobs/station_counts.py data/incoming/trips_2019-06.csv out.csv

spark-submit puts the script's folder first on sys.path. A folder of its own
keeps the bikes modules (config.py, files.py, ...) off it under bare names.
"""

from bikes.station_counts import main

if __name__ == "__main__":
    main()
