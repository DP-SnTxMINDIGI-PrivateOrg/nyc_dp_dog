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
from tmlt.analytics.query_expr import CountMechanism


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
