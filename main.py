import os
import sys

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# --- Tumult Analytics API (Decision 1: Tumult over raw OpenDP / hand-rolled) ---
from tmlt.analytics.session import Session
from tmlt.analytics.query_builder import QueryBuilder
from tmlt.analytics.keyset import KeySet
from tmlt.analytics.privacy_budget import PureDPBudget  # ApproxDPBudget: see Decision 5
from tmlt.analytics.protected_change import AddOneRow    # AddMaxRows: see Decision 3
from tmlt.analytics import CountMechanism

# ===========================================================================
# Decision — PureDP now, ApproxDP door left open.
# BUDGET_CLS is the single swap point. Moving to (epsilon, delta) DP means:
#   1. from tmlt.analytics.privacy_budget import ApproxDPBudget
#   2. BUDGET_CLS = ApproxDPBudget      (and give budget() a delta below)
# QueryBuilder / KeySet / evaluate() do not depend on which budget type is used,
# so nothing else in this file changes.
# ===========================================================================
BUDGET_CLS = PureDPBudget


def budget(epsilon):
    """Construct a per-spend budget object. One place to change for ApproxDP."""
    return BUDGET_CLS(epsilon)
    # ApproxDP variant:  return BUDGET_CLS(epsilon, DELTA)

# ---------------------------------------------------------------------------
# Constants for the Dog demo. These are public-domain, so they are not a privacy risk to hard-code.
# ---------------------------------------------------------------------------
POPULAR_NAMES = [
    "BELLA", "MAX", "CHARLIE", "COCO", "LOLA", "LUCKY", "ROCKY", "BUDDY",
    "PRINCESS", "LUCY", "DAISY", "TEDDY", "LILY", "GINGER", "MOLLY", "JACK",
    "TOBY", "BAILEY", "ANGEL", "ZEUS",
]

COMMON_BREEDS = [
    "YORKSHIRE TERRIER", "SHIH TZU", "CHIHUAHUA", "MALTESE",
    "LABRADOR RETRIEVER", "PIT BULL", "GERMAN SHEPHERD DOG", "POODLE",
    "BEAGLE", "DACHSHUND", "GOLDEN RETRIEVER", "COCKER SPANIEL", "PUG",
    "BOXER", "HAVANESE", "FRENCH BULLDOG", "BICHON FRISE", "MALTESE CROSS",
    "AMERICAN PIT BULL TERRIER/PIT BULL", "MIXED/OTHER",
]

# ---------------------------------------------------------------------------
# .env helpers
# ---------------------------------------------------------------------------
def env_str(name, default):
    return os.getenv(name, default)

def env_float(name, default):
    return float(os.getenv(name, str(default)))

def env_bool(name, default):
    return os.getenv(name, str(default)).strip().lower() in ("1", "true", "yes", "on")

def env_list(name, default):
    raw = os.getenv(name, default)
    return [x.strip() for x in raw.split(",") if x.strip()]


def sql_in_clause(column, values):
    """Build a Spark-SQL IN (...) predicate from a list of string values."""
    quoted = ", ".join("'{}'".format(v.replace("'", "''")) for v in values)
    return "{} IN ({})".format(column, quoted)


def main():
    load_dotenv()

    # --- Configuration (all .env-driven, zero code edits) ------------------
    CSV_PATH = env_str("CSV_PATH", "/data/nyc_dog_licenses.csv")

    # Decision 6 — teaching allocation, NOT a production recommendation.
    # 3 questions x 0.5 = 1.5 spent of 3.0 lifetime; the 1.5 headroom lets you
    # re-run or add a 4th question in a demo without restarting the Session.
    # In production, size TOTAL to the sum of anticipated queries; leftover
    # budget is wasted privacy risk, not a convenience buffer.
    TOTAL_EPSILON = env_float("DP_TOTAL_EPSILON", 3.0)
    PER_QUERY_EPSILON = env_float("DP_PER_QUERY_EPSILON", 0.5)

    # Decision 7 — SHOW_EXACT is ONLY legitimate because this dataset is public.
    # Never enable this for confidential data: it prints the true answer beside
    # the DP answer, which is a real privacy leak on non-public data.
    SHOW_EXACT = env_bool("SHOW_EXACT", True)

    Q1_BREED = env_str("Q1_BREED", "CHIHUAHUA").upper()
    Q1_ZIP = env_str("Q1_ZIP", "10022")

    Q3_BOROUGH = env_str("Q3_BOROUGH", "Queens")
    # Borough -> ZIP set. The dataset has no borough column, so a borough is a
    # public list of ZIP codes. Default is Queens; override via .env.
    Q3_ZIPS = env_list(
        "Q3_ZIPS",
        "11004,11005,11101,11102,11103,11104,11105,11106,11109,11351,11354,"
        "11355,11356,11357,11358,11359,11360,11361,11362,11363,11364,11365,"
        "11366,11367,11368,11369,11370,11372,11373,11374,11375,11377,11378,"
        "11379,11385,11411,11412,11413,11414,11415,11416,11417,11418,11419,"
        "11420,11421,11422,11423,11426,11427,11428,11429,11432,11433,11434,"
        "11435,11436,11691,11692,11693,11694,11697",
    )

    print("=" * 70)
    print("NYC Dog-License Differential Privacy POC (Tumult Analytics)")
    print("=" * 70)
    print("Privacy model : pure epsilon-DP  ({})".format(BUDGET_CLS.__name__))
    print("Privacy unit  : AddOneRow (one license record)")
    print("Lifetime eps  : {}".format(TOTAL_EPSILON))
    print("Per-query eps : {}".format(PER_QUERY_EPSILON))
    print("Show exact    : {}  (public dataset only)".format(SHOW_EXACT))
    print("-" * 70)

    # --- Spark session ------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("nyc-dog-license-dp")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # --- Load + normalize the raw data --------------------------------------
    # Normalization (uppercase strings, ZipCode as string) is preprocessing of
    # the INPUT before the Session exists, so it costs no privacy budget. It
    # also removes case-mismatch traps where a KeySet value silently matches
    # zero rows.
    # The NYC Open Data export is a semicolon-delimited CSV (not comma) and
    # carries a UTF-8 BOM on the first header token. Tell Spark both.
    raw = spark.read.csv(
        CSV_PATH,
        header=True,
        inferSchema=True,
        sep=";",
    )

    # Defensive: strip UTF-8 BOM / stray whitespace from header names so
    # F.col("AnimalName") resolves.
    raw = raw.toDF(*[c.lstrip("\ufeff").strip() for c in raw.columns])

    raw = (
        raw
        .withColumn("AnimalName", F.upper(F.trim(F.col("AnimalName").cast("string"))))
        .withColumn("BreedName", F.upper(F.trim(F.col("BreedName").cast("string"))))
        .withColumn("ZipCode", F.trim(F.col("ZipCode").cast("string")))
    )

    # --- Build the DP Session (the stateful budget ledger) ------------------
    # Decision  — AddOneRow protects exactly one license *record*. The schema
    # has no stable per-dog/per-owner id, so a stronger AddMaxRows(k) would
    # claim more than the data can back. Swap point (stronger, coarser):
    #   from tmlt.analytics.protected_change import AddMaxRows
    #   protected_change=AddMaxRows(3)
    session = Session.from_dataframe(
        privacy_budget=budget(TOTAL_EPSILON),
        source_id="dogs",
        dataframe=raw,
        protected_change=AddOneRow(),
    )

    spent = 0.0

    def ledger():
        remaining = TOTAL_EPSILON - spent
        print("   [budget] spent eps={:.3f}  remaining eps={:.3f}".format(spent, remaining))

    # =======================================================================
    # Q1 — filter().count()   (Decision 2: simplest shape, scalar Laplace)
    # Sensitivity = 1, noise scale = 1/epsilon. filter() is a stability-1
    # transformation and costs ZERO budget; only count() spends.
    # =======================================================================
    print("\nQ1. How many {}s are in ZIP {}?".format(Q1_BREED.title(), Q1_ZIP))
    q1 = (
        QueryBuilder("dogs")
        .filter("BreedName = '{}'".format(Q1_BREED))
        .filter("ZipCode = '{}'".format(Q1_ZIP))
        .count(mechanism=CountMechanism.LAPLACE)
    )
    q1_dp = session.evaluate(q1, budget(PER_QUERY_EPSILON)).collect()[0]["count"]
    spent += PER_QUERY_EPSILON
    print("   DP answer   : {}".format(q1_dp))
    if SHOW_EXACT:
        q1_exact = raw.filter(
            (F.col("BreedName") == Q1_BREED) & (F.col("ZipCode") == Q1_ZIP)
        ).count()
        print("   exact       : {}  (public data only)".format(q1_exact))
    ledger()

    # =======================================================================
    # Q2 — groupby(KeySet).count() + argmax   (Decision 2: histogram pattern)
    # KeySet declares the public domain of names. argmax is post-processing of
    # an already-private histogram, so it is FREE — no extra budget.
    # =======================================================================
    print("\nQ2. What is the most popular dog name?")
    names_keyset = KeySet.from_dict({"AnimalName": POPULAR_NAMES})
    q2 = (
        QueryBuilder("dogs")
        .groupby(names_keyset)
        .count(mechanism=CountMechanism.LAPLACE)
    )
    q2_hist = session.evaluate(q2, budget(PER_QUERY_EPSILON))
    spent += PER_QUERY_EPSILON
    q2_top = q2_hist.orderBy(F.desc("count")).first()
    print("   DP answer   : {}  (noisy count {})".format(q2_top["AnimalName"], q2_top["count"]))
    if SHOW_EXACT:
        exact_top = (
            raw.filter(F.col("AnimalName").isin(POPULAR_NAMES))
            .groupBy("AnimalName").count()
            .orderBy(F.desc("count")).first()
        )
        print("   exact       : {}  (count {})  (public data only)".format(
            exact_top["AnimalName"], exact_top["count"]))
    ledger()

    # =======================================================================
    # Q3 — filter().groupby(KeySet).count() + argmax   (Decision 2: both at once)
    # Proves filter (Queens ZIPs) composes with a group-by histogram without any
    # extra budget cost beyond the single count() spend.
    # =======================================================================
    print("\nQ3. What is the most common breed in {}?".format(Q3_BOROUGH))
    breeds_keyset = KeySet.from_dict({"BreedName": COMMON_BREEDS})
    q3 = (
        QueryBuilder("dogs")
        .filter(sql_in_clause("ZipCode", Q3_ZIPS))
        .groupby(breeds_keyset)
        .count(mechanism=CountMechanism.LAPLACE)
    )
    q3_hist = session.evaluate(q3, budget(PER_QUERY_EPSILON))
    spent += PER_QUERY_EPSILON
    q3_top = q3_hist.orderBy(F.desc("count")).first()
    print("   DP answer   : {}  (noisy count {})".format(q3_top["BreedName"], q3_top["count"]))
    if SHOW_EXACT:
        exact_breed = (
            raw.filter(F.col("ZipCode").isin(Q3_ZIPS))
            .filter(F.col("BreedName").isin(COMMON_BREEDS))
            .groupBy("BreedName").count()
            .orderBy(F.desc("count")).first()
        )
        print("   exact       : {}  (count {})  (public data only)".format(
            exact_breed["BreedName"], exact_breed["count"]))
    ledger()

    print("\n" + "=" * 70)
    print("Done. Spent eps={:.3f} of {:.3f} lifetime budget.".format(spent, TOTAL_EPSILON))
    print("=" * 70)

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # noqa: BLE001 - surface config/data errors clearly
        print("ERROR: {}".format(exc), file=sys.stderr)
        raise