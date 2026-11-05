// The three queries the task asks for, run in MongoDB itself.
//
// Paste each pipeline into Compass's Aggregations tab (collection
// reviews.tiktok_reviews), or run the whole file with the mongosh that ships in
// the mongo container. The credentials come from the container's own
// environment, so no password is typed:
//
//     docker compose cp mongo/aggregations.js mongo:/tmp/aggregations.js
//     docker compose exec -T mongo sh -c 'mongosh --quiet -u "$MONGO_INITDB_ROOT_USERNAME" -p "$MONGO_INITDB_ROOT_PASSWORD" --authenticationDatabase admin /tmp/aggregations.js' > docs/mongo_results.txt
//
// Expected values are recorded next to each query. They were computed
// independently in pandas from the same processed file, and the MongoDB output
// in docs/mongo_results.txt matches them.

const reviews = db.getSiblingDB("reviews").tiktok_reviews;

print("documents in collection: " + reviews.countDocuments({}));
print("expected:                307057\n");

// ---------------------------------------------------------------------------
// 1. Top-5 most frequent comments
//
// Expected: Good 12164, Nice 10050, "-" 7338, "Nice app" 4950, "Good app" 3678
//
// "-" placing third is not a bug. 7,320 reviews consisted only of emoji, so
// stripping symbols left them empty, and the null rule turns an empty value into
// "-"; the other 18 were null words ("None", "N/A", "Null") replaced in stage 1.
// Counts for real comments also rose against the raw file ("Good" 11452 ->
// 12164) because "Good 😊" and "Good" collapse into the same string once the
// emoji is gone.
//
// _id is the secondary sort key so the order is stable when counts tie.
// ---------------------------------------------------------------------------
print("--- 1. top five most frequent comments ---");
printjson(
  reviews.aggregate([
    { $group: { _id: "$content", count: { $sum: 1 } } },
    { $sort: { count: -1, _id: 1 } },
    { $limit: 5 },
  ]).toArray()
);

// The same question asked about genuine comments only. Worth showing both: the
// first answers the task literally, this one answers what the task is for.
//
// Expected: Good 12164, Nice 10050, "Nice app" 4950, "Good app" 3678, Great 2288
print("\n--- 1b. same, excluding the null placeholder ---");
printjson(
  reviews.aggregate([
    { $match: { content: { $ne: "-" } } },
    { $group: { _id: "$content", count: { $sum: 1 } } },
    { $sort: { count: -1, _id: 1 } },
    { $limit: 5 },
  ]).toArray()
);

// ---------------------------------------------------------------------------
// 2. Every record whose content is shorter than 5 characters
//
// Expected: 53238 documents.
//
// $strLenCP, not $strLenBytes. CP counts code points — characters — which is
// what "менее 5 символов" asks for. $strLenBytes counts UTF-8 bytes, so a
// four-character Cyrillic or CJK review measures 8 or 12 bytes and would be
// missed. The two agree only for ASCII.
//
// This predicate cannot use an index: $strLenCP is computed per document at
// query time, so it is always a collection scan. An index would only help if the
// length were stored as its own field at load time.
// ---------------------------------------------------------------------------
print("\n--- 2. content shorter than five characters ---");
print("matching documents: " + reviews.countDocuments({
  $expr: { $lt: [{ $strLenCP: "$content" }, 5] },
}));

print("first ten:");
printjson(
  reviews.aggregate([
    { $match: { $expr: { $lt: [{ $strLenCP: "$content" }, 5] } } },
    { $project: { _id: 1, content: 1, score: 1, created_date: 1 } },
    { $sort: { created_date: 1 } },
    { $limit: 10 },
  ]).toArray()
);

// ---------------------------------------------------------------------------
// 3. Average rating per day, the key as a timestamp
//
// Expected: 79 days, 2022-01-17 to 2022-04-05, overall mean score 4.306.
//
// $dateTrunc returns a BSON Date, which is the "timestamp type" the task asks
// for. Grouping on a formatted string ($dateToString) would return text instead,
// and would also sort lexicographically rather than chronologically.
//
// This works because the loader converts created_date to a real datetime before
// inserting. Had it stayed a string, $dateTrunc would error.
//
// No timezone argument, so days are UTC — matching the source timestamps, which
// carry no offset. Add timezone: "Europe/Minsk" to bucket by local day instead;
// that moves the boundaries and changes the per-day averages.
//
// $dateTrunc needs MongoDB 5.0+. On anything older, group on
// { $dateFromParts: { year: { $year: "$created_date" }, month: ..., day: ... } }.
// ---------------------------------------------------------------------------
print("\n--- 3. average rating per day ---");
printjson(
  reviews.aggregate([
    {
      $group: {
        _id: { $dateTrunc: { date: "$created_date", unit: "day" } },
        average_rating: { $avg: "$score" },
        reviews: { $sum: 1 },
      },
    },
    { $sort: { _id: 1 } },
  ]).toArray()
);
