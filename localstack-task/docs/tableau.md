# Tableau Public dashboards

Tableau Public cannot reach LocalStack, so the dashboards read CSV files
exported from the metrics tables:

```
python -m bikes.export    # into data/export/
```

| File | A row is | Columns |
|---|---|---|
| `bike_daily_metrics.csv` | a day | Date, Trips, Valid trips, Avg distance (km), Avg duration (min), Avg speed (km/h), Avg temperature (degC), Temperature hours |
| `bike_monthly_metrics.csv` | a month | Month (its first day), Days with trips, then the same as a day |
| `bike_station_metrics.csv` | a station in a month | Month, Station, Departures, Returns |

The averages cover valid trips only ([dynamodb.md](dynamodb.md)); a day
without any has empty cells. Charts by month use the monthly file: averaging
the daily rows would weigh a quiet day like a busy one.

## The dashboards

Built in Tableau Public's web authoring, with the daily and the monthly file
as data sources:

- [By day](https://public.tableau.com/views/Helsinkicitybikes/Byday): Trips per day, with Date as a continuous day,
  above Daily Averages, the four averages on Rows with an axis each.
- [By month](https://public.tableau.com/views/Helsinkicitybikes/Bymonth): the same with the continuous Month.

They are bar charts because there are no trips in winter: a line would join
October to the next spring.

If the numbers load as text, Tableau read the files with a locale that
expects a decimal comma; set the data source's locale to English.
