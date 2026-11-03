# import os
# import sys

# from dotenv import load_dotenv
# from pyspark.sql import SparkSession
# from pyspark.sql import functions as F

# # --- Tumult Analytics API ---
# from tmlt.analytics.session import Session
# from tmlt.analytics.query_builder import QueryBuilder
# from tmlt.analytics.keyset import KeySet
# from tmlt.analytics.privacy_budget import PureDPBudget  # ApproxDPBudget: see Decision 5
# from tmlt.analytics.protected_change import AddOneRow    # AddMaxRows: see Decision 3
# from tmlt.analytics import CountMechanism

# # ===========================================================================
# # Decision — PureDP now, ApproxDP door left open for next.
# # BUDGET_CLS is the single swap point. Moving to (epsilon, delta) DP means:
# #   1. from tmlt.analytics.privacy_budget import ApproxDPBudget
# #   2. BUDGET_CLS = ApproxDPBudget      (and give budget() a delta below)
# # QueryBuilder / KeySet / evaluate() do not depend on which budget type is used,
# # so nothing else in this file changes.
# # ===========================================================================
# BUDGET_CLS = PureDPBudget


# def budget(epsilon):
#     """Construct a per-spend budget object"""
#     return BUDGET_CLS(epsilon)
#     # ApproxDP variant:  return BUDGET_CLS(epsilon, DELTA)

# # ---------------------------------------------------------------------------
# # Constants for the Dog demo. These are public-domain, so they are not a privacy risk to hard-code.
# # ---------------------------------------------------------------------------
# POPULAR_NAMES = [
#     "BELLA", "MAX", "CHARLIE", "COCO", "LOLA", "LUCKY", "ROCKY", "BUDDY",
#     "PRINCESS", "LUCY", "DAISY", "TEDDY", "LILY", "GINGER", "MOLLY", "JACK",
#     "TOBY", "BAILEY", "ANGEL", "ZEUS",
# ]

# COMMON_BREEDS = [
#     "YORKSHIRE TERRIER", "SHIH TZU", "CHIHUAHUA", "MALTESE",
#     "LABRADOR RETRIEVER", "PIT BULL", "GERMAN SHEPHERD DOG", "POODLE",
#     "BEAGLE", "DACHSHUND", "GOLDEN RETRIEVER", "COCKER SPANIEL", "PUG",
#     "BOXER", "HAVANESE", "FRENCH BULLDOG", "BICHON FRISE", "MALTESE CROSS",
#     "AMERICAN PIT BULL TERRIER/PIT BULL", "MIXED/OTHER",
# ]

# # ---------------------------------------------------------------------------
# # .env helpers
# # ---------------------------------------------------------------------------
# def env_str(name, default):
#     return os.getenv(name, default)

# def env_float(name, default):
#     return float(os.getenv(name, str(default)))

# def env_bool(name, default):
#     return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")

# def env_list(name, default):
#     raw = os.getenv(name, default)
#     return [x.strip() for x in raw.split(",") if x.strip()]


# def sql_in_clause(column, values):
#     """Build a Spark-SQL IN (...) predicate from a list of string values."""
#     quoted = ", ".join("'{}'".format(v.replace("'", "''")) for v in values)
#     return "{} IN ({})".format(column, quoted)


# def main():
#     load_dotenv()

#     # --- Configuration (all .env-driven, zero code edits) ------------------
#     CSV_PATH = env_str("CSV_PATH", "/data/nyc_dog_licenses.csv")

#     # Note — teaching allocation, NOT a production recommendation.
#     # 3 questions x 0.5 = 1.5 spent of 3.0 lifetime; the 1.5 headroom lets you
#     # re-run or add a 4th question in a demo without restarting the Session.
#     # In production, size TOTAL to the sum of anticipated queries; leftover
#     # budget is wasted privacy risk, not a convenience buffer.
#     TOTAL_EPSILON = env_float("DP_TOTAL_EPSILON", 3.0)
#     PER_QUERY_EPSILON = env_float("DP_PER_QUERY_EPSILON", 0.5)

#     # Never enable this for confidential data: it prints the true answer beside
#     # the DP answer, which is a real privacy leak on non-public data.
#     SHOW_EXACT = env_bool("SHOW_EXACT", True)

#     Q1_BREED = env_str("Q1_BREED", "CHIHUAHUA").upper()
#     Q1_ZIP = env_str("Q1_ZIP", "10022")

#     Q3_BOROUGH = env_str("Q3_BOROUGH", "Queens")
#     # Borough -> ZIP set. The dataset has no borough column, so a borough is a
#     # public list of ZIP codes. Default is Queens; override via .env.
#     Q3_ZIPS = env_list(
#         "Q3_ZIPS",
#         "11004,11005,11101,11102,11103,11104,11105,11106,11109,11351,11354,"
#         "11355,11356,11357,11358,11359,11360,11361,11362,11363,11364,11365,"
#         "11366,11367,11368,11369,11370,11372,11373,11374,11375,11377,11378,"
#         "11379,11385,11411,11412,11413,11414,11415,11416,11417,11418,11419,"
#         "11420,11421,11422,11423,11426,11427,11428,11429,11432,11433,11434,"
#         "11435,11436,11691,11692,11693,11694,11697",
#     )

#     print("=" * 70)
#     print("NYC Dog-License Differential Privacy POC (Tumult Analytics)")
#     print("=" * 70)
#     print("Privacy model : pure epsilon-DP  ({})".format(BUDGET_CLS.__name__))
#     print("Privacy unit  : AddOneRow (one license record)")
#     print("Lifetime eps  : {}".format(TOTAL_EPSILON))
#     print("Per-query eps : {}".format(PER_QUERY_EPSILON))
#     print("Show exact    : {}  (public dataset only)".format(SHOW_EXACT))
#     print("-" * 70)

#     # --- Spark session ------------------------------------------------------
#     spark = (
#         SparkSession.builder
#         .appName("nyc-dog-license-dp")
#         .getOrCreate()
#     )
#     spark.sparkContext.setLogLevel("ERROR")

#     # --- Load + normalize the raw data --------------------------------------
#     # Normalization (uppercase strings, ZipCode as string) is preprocessing of
#     # the INPUT before the Session exists, so it costs no privacy budget. It
#     # also removes case-mismatch traps where a KeySet value silently matches
#     # zero rows.
#     # The NYC Open Data export is a semicolon-delimited CSV (not comma) and
#     # carries a UTF-8 BOM on the first header token. Tell Spark both.
#     raw = spark.read.csv(
#         CSV_PATH,
#         header=True,
#         inferSchema=True,
#         sep=";",
#     )

#     # strip UTF-8 BOM / stray whitespace from header names so
#     # F.col("AnimalName")
#     raw = raw.toDF(*[c.lstrip("\ufeff").strip() for c in raw.columns])

#     raw = (
#         raw
#         .withColumn("AnimalName", F.upper(F.trim(F.col("AnimalName").cast("string"))))
#         .withColumn("BreedName", F.upper(F.trim(F.col("BreedName").cast("string"))))
#         .withColumn("ZipCode", F.trim(F.col("ZipCode").cast("string")))
#     )

#     # --- Build the DP Session (the stateful budget ledger) ------------------
#     # Decision  — AddOneRow protects exactly one license *record*. The schema
#     # has no stable per-dog/per-owner id, so a stronger AddMaxRows(k) would
#     # claim more than the data can back. Swap point (stronger, coarser):
#     #   from tmlt.analytics.protected_change import AddMaxRows
#     #   protected_change=AddMaxRows(3)
#     session = Session.from_dataframe(
#         privacy_budget=budget(TOTAL_EPSILON),
#         source_id="dogs",
#         dataframe=raw,
#         protected_change=AddOneRow(),
#     )

#     spent = 0.0

#     def ledger():
#         remaining = TOTAL_EPSILON - spent
#         print("   [budget] spent eps={:.3f}  remaining eps={:.3f}".format(spent, remaining))

#     # =======================================================================
#     # Q1 — filter().count()   
#     # Sensitivity = 1, noise scale = 1/epsilon. filter() is a stability-1
#     # transformation and costs ZERO budget; only count() spends.
#     # =======================================================================
#     print("\nQ1. How many {}s are in ZIP {}?".format(Q1_BREED.title(), Q1_ZIP))
#     q1 = (
#         QueryBuilder("dogs")
#         .filter("BreedName = '{}'".format(Q1_BREED))
#         .filter("ZipCode = '{}'".format(Q1_ZIP))
#         .count(mechanism=CountMechanism.LAPLACE)
#     )
#     q1_dp = session.evaluate(q1, budget(PER_QUERY_EPSILON)).collect()[0]["count"]
#     spent += PER_QUERY_EPSILON
#     print("   DP answer   : {}".format(q1_dp))
#     if SHOW_EXACT:
#         q1_exact = raw.filter(
#             (F.col("BreedName") == Q1_BREED) & (F.col("ZipCode") == Q1_ZIP)
#         ).count()
#         print("   exact       : {}  (public data only)".format(q1_exact))
#     ledger()

#     # =======================================================================
#     # Q2 — groupby(KeySet).count() + argmax  
#     # KeySet declares the public domain of names. argmax is post-processing of
#     # an already-private histogram, so it is FREE — no extra budget.
#     # =======================================================================
#     print("\nQ2. What is the most popular dog name?")
#     names_keyset = KeySet.from_dict({"AnimalName": POPULAR_NAMES})
#     q2 = (
#         QueryBuilder("dogs")
#         .groupby(names_keyset)
#         .count(mechanism=CountMechanism.LAPLACE)
#     )
#     q2_hist = session.evaluate(q2, budget(PER_QUERY_EPSILON))
#     spent += PER_QUERY_EPSILON
#     q2_top = q2_hist.orderBy(F.desc("count")).first()
#     print("   DP answer   : {}  (noisy count {})".format(q2_top["AnimalName"], q2_top["count"]))
#     if SHOW_EXACT:
#         exact_top = (
#             raw.filter(F.col("AnimalName").isin(POPULAR_NAMES))
#             .groupBy("AnimalName").count()
#             .orderBy(F.desc("count")).first()
#         )
#         print("   exact       : {}  (count {})  (public data only)".format(
#             exact_top["AnimalName"], exact_top["count"]))
#     ledger()

#     # =======================================================================
#     # Q3 — filter().groupby(KeySet).count() + argmax  
#     # Proves filter (Queens ZIPs) composes with a group-by histogram without any
#     # extra budget cost beyond the single count() spend.
#     # =======================================================================
#     print("\nQ3. What is the most common breed in {}?".format(Q3_BOROUGH))
#     breeds_keyset = KeySet.from_dict({"BreedName": COMMON_BREEDS})
#     q3 = (
#         QueryBuilder("dogs")
#         .filter(sql_in_clause("ZipCode", Q3_ZIPS))
#         .groupby(breeds_keyset)
#         .count(mechanism=CountMechanism.LAPLACE)
#     )
#     q3_hist = session.evaluate(q3, budget(PER_QUERY_EPSILON))
#     spent += PER_QUERY_EPSILON
#     q3_top = q3_hist.orderBy(F.desc("count")).first()
#     print("   DP answer   : {}  (noisy count {})".format(q3_top["BreedName"], q3_top["count"]))
#     if SHOW_EXACT:
#         exact_breed = (
#             raw.filter(F.col("ZipCode").isin(Q3_ZIPS))
#             .filter(F.col("BreedName").isin(COMMON_BREEDS))
#             .groupBy("BreedName").count()
#             .orderBy(F.desc("count")).first()
#         )
#         print("   exact       : {}  (count {})  (public data only)".format(
#             exact_breed["BreedName"], exact_breed["count"]))
#     ledger()

#     print("\n" + "=" * 70)
#     print("Done. Spent eps={:.3f} of {:.3f} lifetime budget.".format(spent, TOTAL_EPSILON))
#     print("=" * 70)

#     spark.stop()


# if __name__ == "__main__":
#     try:
#         main()
#     except Exception as exc:  # noqa: BLE001 - surface config/data errors clearly
#         print("ERROR: {}".format(exc), file=sys.stderr)
#         raise

"""Simple NYC dog-licence DP POC: original three questions + ten categories.

All configurable inputs live in .env. The application stays in one main.py.
--list prints questions/specifications; --plan prints budget; neither reads CSV.
"""
import argparse
import math
import os
import re
from pathlib import Path


def env_list(name):
    values = [v.strip().upper() for v in os.getenv(name, "").split(",") if v.strip()]
    if not values or len(values) != len(set(values)):
        raise ValueError(name + " must contain nonempty, unique public values")
    return values


def positive(name, default):
    value = float(os.getenv(name, default))
    if not math.isfinite(value) or value <= 0:
        raise ValueError(name + " must be finite and positive")
    return value


def flag(name):
    text = os.getenv(name, "false").strip().lower()
    if text not in {"true", "false"}:
        raise ValueError(name + " must be true or false")
    return text == "true"


def settings():
    c = {
        "path": os.getenv("CSV_PATH", "/data/nyc_dog_licenses.csv"),
        "sep": os.getenv("CSV_SEP", ";"),
        "date_format": os.getenv("DATE_FORMAT", "MM/dd/yyyy"),
        "master": os.getenv("SPARK_MASTER", "local[2]"),
        "epsilon": positive("DP_PER_QUERY_EPSILON", "1"),
        "cap": os.getenv("DP_TOTAL_EPSILON", "auto").strip().lower(),
        "breed": os.getenv("Q1_BREED", "CHIHUAHUA").strip().upper(),
        "zip": os.getenv("Q1_ZIP", "10022").strip(),
        "region": os.getenv("Q3_BOROUGH", "Queens").strip(),
        "zips": env_list("Q3_ZIPS"),
        "names": env_list("PUBLIC_NAMES"),
        "breeds": env_list("PUBLIC_BREEDS"),
        "genders": env_list("PUBLIC_GENDERS"),
        "gender_other": os.getenv("GENDER_OTHER_BUCKET", "OTHER").strip().upper(),
        "unknown_breeds": env_list("UNKNOWN_BREED_VALUES"),
        "low": float(os.getenv("AGE_LOW", "0")),
        "high": float(os.getenv("AGE_HIGH", "25")),
        "quantile": float(os.getenv("AGE_QUANTILE", "0.5")),
        "top_k": int(os.getenv("TOP_K", "5")),
        "year_start": int(os.getenv("ISSUE_YEAR_START", "2014")),
        "year_end": int(os.getenv("ISSUE_YEAR_END", "2026")),
        "extract": os.getenv("EXTRACT_YEAR_FILTER", "").strip(),
        "minimum": positive("MIN_NOISY_DENOMINATOR", "10"),
        "exact": flag("SHOW_EXACT"),
        "public": flag("PUBLIC_DATA"),
    }
    selection = os.getenv("RUN_QUESTIONS", "all").strip().lower()
    c["questions"] = list(range(1, 14)) if selection == "all" else sorted(set(int(q.strip()) for q in selection.split(",")))
    if not c["questions"] or any(q not in range(1, 14) for q in c["questions"]):
        raise ValueError("RUN_QUESTIONS must be all or comma-separated IDs 1-13")
    if not all(math.isfinite(x) for x in [c["low"], c["high"], c["quantile"]]):
        raise ValueError("Bounds and quantile must be finite")
    if not 0 <= c["low"] < c["high"] or not 0 <= c["quantile"] <= 1:
        raise ValueError("Require 0 <= AGE_LOW < AGE_HIGH; AGE_QUANTILE in [0,1]")
    if not 1 <= c["top_k"] <= len(c["breeds"]):
        raise ValueError("TOP_K must be between 1 and the public breed-list length")
    if not 1 <= c["year_start"] <= c["year_end"] <= 9999 or c["year_end"] - c["year_start"] > 199:
        raise ValueError("Require a public issue-year window of at most 200 years")
    if c["gender_other"] in c["genders"] or not c["gender_other"]:
        raise ValueError("GENDER_OTHER_BUCKET must be nonempty and distinct from PUBLIC_GENDERS")
    if c["extract"] and not 1 <= int(c["extract"]) <= 9999:
        raise ValueError("EXTRACT_YEAR_FILTER must be empty or a valid calendar year")
    if c["exact"] and not c["public"]:
        raise ValueError("SHOW_EXACT=true is allowed only with PUBLIC_DATA=true on already-public input")
    return c


def questions(c):
    age = "age proxy (ExtractYear minus AnimalBirthYear), clamped to [{:g}, {:g}] years".format(c["low"], c["high"])
    return {
        1: ("Original: filtered count", "How many licence records list {} in ZIP {}?".format(c["breed"], c["zip"]),
            "Filter the configured breed and ZIP; release one noisy count."),
        2: ("Original: top name", "Among the public candidate names, which name is most common?",
            "Release one name histogram; choose its largest noisy count, breaking ties alphabetically."),
        3: ("Original: regional top breed", "Among the public candidate breeds, which breed is most common in the configured {} ZIP region?".format(c["region"]),
            "Q3_ZIPS determines membership; Q3_BOROUGH is only a label. Release one breed histogram and select its noisy winner."),
        4: ("1. Count", "How many licence records are in the selected input?",
            "One DP count of all retained rows; not a count of established unique dogs or owners."),
        5: ("2. Histogram", "How many records fall into each public gender category and the {} bucket?".format(c["gender_other"]),
            "One DP count histogram. Nonlisted, blank and missing gender values enter the configured other bucket."),
        6: ("3. Sum", "What is the total {} across the records?".format(age),
            "One bounded DP sum, excluding missing ages. Units: licence-record-years, not observed follow-up time."),
        7: ("4. Average", "What is the mean {}?".format(age),
            "One bounded Tumult average query on records with a usable age."),
        8: ("5. Quantile", "What is quantile {:g} of the {}?".format(c["quantile"], age),
            "One bounded DP quantile query; 0.5 means median and 0.9 means 90th percentile. The result is randomized and can be fractional."),
        9: ("6. Dispersion", "What is the sample standard deviation of the {}?".format(age),
            "One bounded DP stdev query, in years. This is not uncertainty around the DP mean."),
        10: ("7. Ranking", "Which {} public candidate breeds have the highest noisy record counts citywide?".format(c["top_k"]),
             "One candidate-breed histogram, followed by top-k selection. Rankings do not cover breeds outside PUBLIC_BREEDS."),
        11: ("8. Ratio / proportion", "What proportion of records have an unknown or missing breed?",
             "One unknown/known DP histogram. Unknown means a value in UNKNOWN_BREED_VALUES or a null/blank breed. Clip noisy counts at zero and divide unknown by their sum; n/a if that sum is too small."),
        12: ("9. Time series", "How many record appearances have an issue year in each year from {} through {}?".format(c["year_start"], c["year_end"]),
             "One issue-year DP histogram, plus OTHER and MISSING. Counts record appearances, not deduplicated issuances; repeated extracts and partial years can distort trends."),
        13: ("10. Composite", "What mean clamped age is obtained from a DP age sum divided by a DP eligible-record count?",
             "Two evaluate calls share ONE question allocation equally. Both use the same nonmissing-age rows. Divide if the noisy count passes the threshold; clip the quotient to the age bounds. This deliberately re-measures a mean for teaching rather than reusing Q7."),
    }


def sql_string(value):
    return "'" + value.replace("\\", "\\\\").replace("'", "\\'") + "'"


def ratio_result(numerator, denominator, c, bounded_age=False):
    if denominator < c["minimum"]:
        return "n/a (denominator below configured threshold)"
    value = numerator / denominator
    if bounded_age:
        value = min(c["high"], max(c["low"], value))
    return "{:.6g}".format(value)


def show(rows, title="DP answer"):
    print("   " + title + ":")
    for row in rows:
        print("     " + " | ".join("{}={}".format(k, "n/a" if v is None else v) for k, v in row.items()))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", default=".env")
    parser.add_argument("--list", action="store_true")
    parser.add_argument("--plan", action="store_true")
    args = parser.parse_args()
    if Path(args.env).exists():
        from dotenv import load_dotenv
        load_dotenv(args.env, override=False)
    c = settings()
    catalogue = questions(c)
    if args.list:
        for q, (category, text, details) in catalogue.items():
            print("\nQ{}. {} [{}]\n   Specification: {}".format(q, text, category, details))
        return

    # Budget arithmetic uses Tumult's exact internal representations so the
    # allocated total really covers every call, even with fractional epsilon.
    from tmlt.analytics import PureDPBudget
    allocation = PureDPBudget(c["epsilon"])
    required = PureDPBudget(allocation.value * len(c["questions"]))
    total = required if c["cap"] == "auto" else PureDPBudget(positive("DP_TOTAL_EPSILON", "1"))
    if total.value < required.value:
        raise ValueError("Selected questions need epsilon {}; cap is {}".format(required.epsilon, total.epsilon))
    print("Questions: {} | evaluate calls: {} | planned epsilon: {} | session cap: {}".format(
        c["questions"], len(c["questions"]) + int(13 in c["questions"]), required.epsilon, total.epsilon))
    print("Protection: one INPUT licence row. Ledger is run-local; reruns compose.")
    print("Extract filter: {} | exact output: {}".format(c["extract"] or "all input extracts", c["exact"]))
    if args.plan:
        return

    from importlib.metadata import version
    if version("tmlt.analytics") != "0.21.0":
        raise ValueError("Use tmlt.analytics==0.21.0 from the supplied requirements")
    from pyspark.sql import SparkSession, functions as F
    from tmlt.analytics import AddOneRow, KeySet, QueryBuilder, Session
    spark = (SparkSession.builder.master(c["master"]).appName("nyc-dog-dp-simple")
             .config("spark.sql.ansi.enabled", "false")
             .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
             .config("spark.sql.shuffle.partitions", "8").getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    session = None
    try:
        # All preprocessing is fixed and row-wise: no data-dependent category
        # discovery, deduplication, min/max estimation or extra raw release.
        df = spark.read.csv(c["path"], header=True, inferSchema=False, sep=c["sep"])
        columns = [re.sub(r"[^a-z0-9]", "", name.lstrip("\ufeff").lower()) for name in df.columns]
        if len(columns) != len(set(columns)):
            raise ValueError("CSV contains duplicate normalized headers")
        df = df.toDF(*columns)
        required_columns = ["animalname", "animalgender", "animalbirthyear", "breedname", "zipcode", "licenseissueddate", "extractyear"]
        if any(name not in columns for name in required_columns):
            raise ValueError("Missing required CSV columns; check delimiter and header")
        for name in ["animalname", "animalgender", "breedname"]:
            df = df.withColumn(name, F.upper(F.trim(F.col(name))))
        for name in ["animalbirthyear", "extractyear"]:
            value = F.when(F.trim(F.col(name)).rlike(r"^[0-9]{1,4}$"), F.col(name).cast("long"))
            df = df.withColumn(name, value)
            df = df.withColumn(name, F.when(F.col(name).between(1, 9999), F.col(name)))
        if c["extract"]:
            df = df.filter(F.col("extractyear") == int(c["extract"]))
        df = (df.withColumn("zipcode", F.trim(F.col("zipcode")))
              .withColumn("Age", (F.col("extractyear") - F.col("animalbirthyear")).cast("double"))
              .withColumn("Gender", F.when(F.col("animalgender").isin(c["genders"]), F.col("animalgender")).otherwise(c["gender_other"]))
              .withColumn("BreedStatus", F.when(F.col("breedname").isNull() | (F.col("breedname") == "") | F.col("breedname").isin(c["unknown_breeds"]), "unknown").otherwise("known"))
              .withColumn("IssueYear", F.year(F.to_date(F.trim(F.col("licenseissueddate")), c["date_format"]))))
        years = list(map(str, range(c["year_start"], c["year_end"] + 1)))
        df = df.withColumn("IssueYear", F.when(F.col("IssueYear").isNull(), "MISSING")
                           .when(F.col("IssueYear").between(c["year_start"], c["year_end"]), F.col("IssueYear").cast("string"))
                           .otherwise("OTHER"))
        df = df.select("animalname", "breedname", "zipcode", "Age", "Gender", "BreedStatus", "IssueYear")
        session = Session.from_dataframe(privacy_budget=total, source_id="dogs", dataframe=df, protected_change=AddOneRow())

        def measure(query, spend=allocation):
            result = session.evaluate(query, privacy_budget=spend)
            remaining = session.remaining_privacy_budget
            spent = PureDPBudget(total.value - remaining.value).epsilon
            print("   Budget: spent={:.10g}; remaining={:.10g}".format(spent, remaining.epsilon))
            return [row.asDict() for row in result.collect()]

        def histogram(column, domain, where=None):
            query = QueryBuilder("dogs")
            if where:
                query = query.filter(where)
            return measure(query.groupby(KeySet.from_dict({column: domain})).count(name="value", mechanism="laplace"))

        def exact_histogram(column, domain, where=None):
            raw = df.filter(where) if where else df
            counts = {r[column]: r["count"] for r in raw.filter(F.col(column).isin(domain)).groupBy(column).count().collect()}
            return [{column: key, "value": counts.get(key, 0)} for key in domain]

        def top(rows, column, k):
            return [dict(row, rank=i) for i, row in enumerate(sorted(rows, key=lambda r: (-r["value"], r[column]))[:k], 1)]

        # One public definition shared by DP aggregates and optional exact checks.
        usable = df.filter(F.col("Age").isNotNull())
        bounded = usable.withColumn("BoundedAge", F.least(F.lit(c["high"]), F.greatest(F.lit(c["low"]), F.col("Age"))))

        for q in c["questions"]:
            category, text, details = catalogue[q]
            print("\nQ{}. {} [{}]\n   Specification: {}".format(q, text, category, details))

            # Original Q1: breed + ZIP count.
            if q == 1:
                where = "breedname = {} AND zipcode = {}".format(sql_string(c["breed"]), sql_string(c["zip"]))
                show(measure(QueryBuilder("dogs").filter(where).count(name="value", mechanism="laplace")))
                if c["exact"]:
                    show([{"value": df.filter(where).count()}], "PUBLIC exact benchmark")

            # Original Q2/Q3, plus one new ranking example Q10.
            elif q in (2, 3, 10):
                column, domain = ("animalname", c["names"]) if q == 2 else ("breedname", c["breeds"])
                where = "zipcode IN ({})".format(",".join(sql_string(z) for z in c["zips"])) if q == 3 else None
                k = c["top_k"] if q == 10 else 1
                show(top(histogram(column, domain, where), column, k))
                if c["exact"]:
                    show(top(exact_histogram(column, domain, where), column, k), "PUBLIC exact benchmark")

            # Category 1: count.
            elif q == 4:
                show(measure(QueryBuilder("dogs").count(name="value", mechanism="laplace")))
                if c["exact"]:
                    show([{"value": df.count()}], "PUBLIC exact benchmark")

            # Category 2: histogram.
            elif q == 5:
                domain = c["genders"] + [c["gender_other"]]
                show(histogram("Gender", domain))
                if c["exact"]:
                    show(exact_histogram("Gender", domain), "PUBLIC exact benchmark")

            # Categories 3-6: sum, average, quantile, sample standard deviation.
            elif q in (6, 7, 8, 9):
                query = QueryBuilder("dogs")
                if q == 8:
                    query = query.quantile("Age", c["quantile"], low=c["low"], high=c["high"], name="value")
                else:
                    operation = {6: "sum", 7: "average", 9: "stdev"}[q]
                    query = getattr(query, operation)("Age", low=c["low"], high=c["high"], name="value", mechanism="laplace")
                show(measure(query))
                if c["exact"]:
                    aggregate = {6: lambda: F.coalesce(F.sum("BoundedAge"), F.lit(0.0)),
                                 7: lambda: F.avg("BoundedAge"),
                                 8: lambda: F.expr("percentile(BoundedAge, {})".format(c["quantile"])),
                                 9: lambda: F.stddev_samp("BoundedAge")}[q]()
                    show([r.asDict() for r in bounded.agg(aggregate.alias("value")).collect()], "PUBLIC exact benchmark")

            # Category 8: one measurement, followed by a ratio.
            elif q == 11:
                rows = histogram("BreedStatus", ["unknown", "known"])
                counts = {r["BreedStatus"]: max(0, r["value"]) for r in rows}
                show(rows)
                print("   DP unknown/missing share (fraction): " + ratio_result(counts["unknown"], sum(counts.values()), c))
                if c["exact"]:
                    ex = {r["BreedStatus"]: r["value"] for r in exact_histogram("BreedStatus", ["unknown", "known"])}
                    print("   PUBLIC exact share, same stability rule: " + ratio_result(ex["unknown"], sum(ex.values()), c))

            # Category 9: issue-year time series.
            elif q == 12:
                show(histogram("IssueYear", years + ["OTHER", "MISSING"]))
                if c["exact"]:
                    show(exact_histogram("IssueYear", years + ["OTHER", "MISSING"]), "PUBLIC exact benchmark")

            # Category 10: two private measurements, one question allocation.
            elif q == 13:
                half = PureDPBudget(allocation.value / 2)
                age_sum = measure(QueryBuilder("dogs").filter("Age IS NOT NULL").sum(
                    "Age", low=c["low"], high=c["high"], name="value", mechanism="laplace"), half)[0]["value"]
                eligible_count = measure(QueryBuilder("dogs").filter("Age IS NOT NULL").count(
                    name="value", mechanism="laplace"), half)[0]["value"]
                print("   DP age sum: {}; DP eligible-record count: {}".format(age_sum, eligible_count))
                print("   DP composite mean: " + ratio_result(age_sum, eligible_count, c, bounded_age=True))
                if c["exact"]:
                    ex = bounded.agg(F.coalesce(F.sum("BoundedAge"), F.lit(0.0)).alias("sum"), F.count("BoundedAge").alias("n")).first()
                    print("   PUBLIC exact composite, same stability rule: " + ratio_result(ex["sum"], ex["n"], c, bounded_age=True))

        print("\nDone. Session remaining: {}".format(session.remaining_privacy_budget))
    finally:
        try:
            if session is not None:
                session.stop()
        finally:
            spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        if flag("TRUSTED_DEBUG"):
            import traceback
            traceback.print_exc()
        print("Stopped: inspect configuration in your trusted environment. A failure may already have spent privacy budget; do not blindly retry.")
        raise SystemExit(1)