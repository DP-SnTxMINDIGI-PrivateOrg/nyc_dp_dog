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


