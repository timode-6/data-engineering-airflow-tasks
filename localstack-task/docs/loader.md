# The loader Lambda

Code: `src/bikes/loader/`. Packaging and deployment: `src/bikes/package.py`,
`src/bikes/deploy.py`.

S3 sends an event for each new month file to the `bike-files` queue, and the
queue triggers `bike-loader` with one message per call. A month is loaded
once both of its files, the trips and the station counts, are in S3.

## What happens to a message

| The event is about | The Lambda | The message |
|---|---|---|
| no month file (S3's test event, another key) | does nothing | deleted |
| a file no longer in S3 | does nothing | deleted |
| a month whose other file is not in S3 yet | waits | back in 30 s |
| a month another call is loading | waits | back in 30 s |
| a month already loaded from these two files | does nothing | deleted |
| a month not loaded from these two files | loads it | deleted |
| a month whose load fails | records the error | back in 6 min |

To wait, the function sets the message's visibility timeout to 30 s and
reports it as a batch item failure (`ReportBatchItemFailures`). After 20
receives, about 10 minutes, the message moves to `bike-files-dlq`.
`python -m bikes.infra redrive` moves it back once the cause is fixed.

## Loading a month once

`bike_loads` records which files each month was loaded from: their S3 ETags,
which change when a file is replaced. Before loading, a call claims the month
with a conditional put. It succeeds only if nobody holds a claim and the month
is not already loaded from the same ETags, so of two racing calls exactly one
wins.

A claim lasts as long as the Lambda may run, 5 minutes, so the claim of a
call that dies runs out before its message comes back. Finishing and failing
are conditional on the call's own `claim_id`. The files are read with
`If-Match` on the claimed ETags, so a load never records files it did not read.

## Loading a month again

A replaced file has a new ETag, so its event loads the month again. Items are
overwritten by key, then whatever the earlier load left and the new files no
longer have is deleted: the month's raw trips, daily metrics and station
counts. Readers never see the month empty.

To load months again on purpose, after a change to the loader:

```
python -m bikes.infra reload 2019-06 2020-03   # or --all
```

It forgets the months in `bike_loads` and queues their trips files' events
again. A month being loaded right now is left alone.

## How long a load takes

The raw trip items, about 10,000 for a big month, are written by 8 threads,
each with its own boto3 resource. With Spark idle, the biggest month (June
2019, 715,131 trips) loads in 53 s: 1 s reading S3, 29 s in pandas, 23 s
writing DynamoDB. `python -m bikes.infra loads` shows these times for every
month.

## Deploying

```
python -m bikes.infra deploy    # after `up`, and after every code change
```

1. The package: pandas and numpy, installed by pip inside the Lambda image
   `public.ecr.aws/lambda/python:3.12` with the runtime's own packages as
   constraints, plus the `bikes` code. pip runs once; its output is kept in
   `build/`. The zip is the same byte for byte for the same code, so an
   unchanged package is not uploaded again.
2. The IAM role. LocalStack does not enforce it; AWS would.
3. The function: python3.12, 2 GB (the biggest month peaks at about 1 GB),
   5 minutes, code from `s3://helsinki-city-bikes/deploy/bike-loader.zip`.
4. The trigger: one message per call, `ReportBatchItemFailures`, at most two
   calls at once.
