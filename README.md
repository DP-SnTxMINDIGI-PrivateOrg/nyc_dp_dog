# NYC Dog Licensing Differential Privacy POC

A proof of concept for running aggregate queries over the NYC Dog Licensing
CSV with Apache Spark and Tumult Analytics. The application demonstrates
row-level differential privacy for scalar counts and grouped histograms.

This repository is a small command-line application: it loads configuration,
reads the CSV, creates one Tumult privacy Session, evaluates three queries,
prints their results and budget usage, then stops Spark.

# nyc-dog-dataset-dp

Dog Dataset DP POC using Tumult Analytics: https://github.com/sahbaazansari/NYC_Dog_DP_Dataset/blob/main/NYC_Dog_Licensing_Dataset_20260911.csv

## What It Does

The current program runs these questions in order:

1. **Q1:** How many licenses match a configured breed and ZIP code?
2. **Q2:** Which name is most common among a fixed list of popular names?
3. **Q3:** Which breed is most common among records whose ZIP code is in a
   configured borough ZIP-code list?

The CSV has a `ZipCode` column but no borough column. Q3 therefore treats a
borough as a configured list of ZIP codes. The list is supplied in `.env`; it
is not a borough value read from the data. Confirm the geographic mapping is
appropriate for your use case before relying on it.

## Processing Flow

<img width="1254" height="756" alt="POC-diagram" src="https://github.com/user-attachments/assets/9b5f47c4-7093-4378-9a95-f5931cf2bc56" />



```text
Docker runs: python main.py
        |
        v
main() loads .env configuration
        |
        v
Spark reads and normalizes the CSV into a DataFrame
        |
        v
Tumult Session is created with a total privacy budget
        |
        +--> Q1: filter breed + ZIP -> count -> DP result
        |
        +--> Q2: group by candidate names -> DP histogram -> top name
        |
        +--> Q3: filter borough ZIPs -> group by candidate breeds
        |       -> DP histogram -> top breed
        |
        v
Print results and budget ledger; stop Spark
```

The CSV is loaded once and used by all three questions. In Docker, the CSV and
`.env` are runtime bind mounts; they are not copied into the image.

## Repository Layout

```text
.
├── Dockerfile
├── main.py
├── README.md
├── requirements.txt
├── .env.example
└── data/
    └── nyc_dog_licenses.csv
```

## Requirements

- Python 3.10 through 3.12 for Tumult Analytics 0.16+.
- A Java runtime when running Spark locally.
- Docker for the containerized setup.

`requirements.txt` contains minimum version requirements, not a fully locked
dependency set:

- `pyspark>=3.3.0`
- `tmlt.analytics>=0.16.0`
- `python-dotenv>=1.0.0`

The Dockerfile uses Eclipse Temurin Java 11, installs Python and the Python
dependencies, and targets `linux/amd64`. It expects the CSV and `.env` to be
mounted when the container runs.

## Configure the Application

Create a working `.env` file from the example:

```bash
cp .env.example .env
```

The settings are:

| Variable | Purpose | Example value |
|---|---|---|
| `CSV_PATH` | CSV path visible to the application | `/data/nyc_dog_licenses.csv` in Docker |
| `DP_TOTAL_EPSILON` | Total PureDP budget for this Session/run | `3.0` |
| `DP_PER_QUERY_EPSILON` | Budget spent for each evaluated question | `0.0001` in the current example |
| `SHOW_EXACT` | Also print exact Spark results when true | `true` |
| `Q1_BREED` | Breed filter for Q1 | `CHIHUAHUA` |
| `Q1_ZIP` | ZIP-code filter for Q1 | `10022` |
| `Q3_BOROUGH` | Label shown for Q3's geographic scope | `Queens` |
| `Q3_ZIPS` | Comma-separated ZIP codes used by Q3 | Queens ZIP list in the example |

`Q3_BOROUGH` is a display label; `Q3_ZIPS` controls which rows Q3 actually
includes. Name and breed candidate domains are currently constants in
`main.py` (`POPULAR_NAMES` and `COMMON_BREEDS`), not environment variables.

## Run with Docker

Run these commands from the repository root. The platform flag is included
because the current Tumult setup targets `linux/amd64`:

```bash
docker build --platform linux/amd64 -t nyc-dog-dp .

docker run --rm --platform linux/amd64 \
  -v "$PWD/data/nyc_dog_licenses.csv:/data/nyc_dog_licenses.csv:ro" \
  -v "$PWD/.env:/app/.env:ro" \
  nyc-dog-dp
```

The container reads the CSV from `/data/nyc_dog_licenses.csv` and the
configuration from `/app/.env`. The volume mounts are read-only.

## Run Locally

Create a Python 3.10–3.12 environment, install dependencies, and ensure Java
is installed and available to Spark:

```bash
virtualenv --python=python3.12 .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

Set `CSV_PATH` in `.env` to the local CSV path, for example:

```dotenv
CSV_PATH=./data/nyc_dog_licenses.csv
```

Then start the application:

```bash
python main.py
```

## Query Design

The queries are implemented directly in `main.py` using Tumult's `QueryBuilder`
and Spark DataFrame operations.

| Query | DP query pattern | Output selection |
|---|---|---|
| Q1 | `filter().filter().count(mechanism=CountMechanism.LAPLACE)` | Collect the private scalar count |
| Q2 | `groupby(KeySet).count(mechanism=CountMechanism.LAPLACE)` over `AnimalName` | Sort the private histogram by count and select the first row |
| Q3 | `filter().groupby(KeySet).count(mechanism=CountMechanism.LAPLACE)` over `ZipCode` and `BreedName` | Sort the private histogram by count and select the first row |

Important functions and APIs in the current implementation:

- `main()` loads configuration, starts Spark, reads the CSV, creates the
  Session, evaluates Q1–Q3, and stops Spark.
- `env_str`, `env_float`, `env_bool`, and `env_list` read configuration values.
- `sql_in_clause(column, values)` builds the ZIP-code predicate used by Q3.
- `budget(epsilon)` creates a `PureDPBudget` for the requested spend.
- `spark.read.csv(...)` reads the semicolon-delimited CSV.
- `Session.from_dataframe(...)` creates the Tumult Session with
  `AddOneRow()` as the protected change.
- `QueryBuilder(...)`, `.filter(...)`, `.groupby(...)`, and `.count(...)`
  define the private queries; `session.evaluate(...)` evaluates each query.
- Spark's `.orderBy(...)` and `.first()` choose a top row from the already
  private histogram; this selection is post-processing of the DP result.

## Privacy Model and Budget

The application uses Tumult Analytics with `PureDPBudget` and
`CountMechanism.LAPLACE`. `Session.from_dataframe(...)` is configured with
`AddOneRow()`, so the declared privacy unit is one license record. The code
does not establish that a row corresponds to one unique dog or owner.

Each `session.evaluate(...)` call spends `DP_PER_QUERY_EPSILON` from
`DP_TOTAL_EPSILON`. The filtering and selection of a top row do not cause
additional spends in the current query patterns; each evaluated count or
histogram does. The Session's budget ledger exists only for the current
process run and is recreated when the program starts again.

### Important Current Example Setting

The current `.env.example` sets:

```dotenv
DP_TOTAL_EPSILON=3.0
DP_PER_QUERY_EPSILON=0.0001
```

With three evaluated questions, the program spends `0.0003` epsilon in one
run, assuming all three complete. The comments in `.env.example` still
describe a `0.5` per-query setting and `1.5` total spend; those comments do
not match the active value. For a sensitivity-1 count, Laplace noise has
scale `1 / epsilon`; epsilon `0.0001` therefore gives a scale of `10,000`,
which can make the released counts extremely noisy. Set the value
intentionally for the demonstration and privacy goal.

`SHOW_EXACT=true` calculates and prints exact results directly from the raw
Spark DataFrame. These exact outputs are not differentially private and do
not pass through `session.evaluate(...)`. Keep this enabled only when exposing
the underlying exact data is acceptable, such as for this public-data demo.

## Adding More Questions

This is a fixed script, not an interactive query service. Additional questions
must be implemented in `main.py` and evaluated against the Tumult Session.
Each additional `session.evaluate(...)` consumes more of the Session's total
budget.

For a grouped answer, define a bounded candidate domain with a Tumult `KeySet`
and aggregate over that domain. For example, a name-frequency query can use
`KeySet.from_dict({"AnimalName": candidate_names})`, then group by that
KeySet and count. A query that returns the most frequent candidate name can
select from the resulting private histogram as post-processing.

**Note:-** Do not return all distinct names discovered from the raw CSV as though they
were protected by the DP query. Data-dependent categories require careful
handling; use an appropriate public/bounded domain and review the privacy
semantics before releasing results.

## Limitations

- A borough is represented by a configured ZIP-code list, not a borough
  attribute in the CSV. Verify that mapping for the intended geographic
  definition.
- Q2 and Q3 only consider values in `POPULAR_NAMES` and `COMMON_BREEDS`.
  Values outside those candidate sets are not included in those histograms.
- `AddOneRow()` protects one record, not necessarily one person, dog, or owner.
- The budget ledger is in memory and does not persist across program runs.
- Exact output enabled by `SHOW_EXACT` bypasses differential privacy.
- This repository is a proof of concept, not a production privacy review or
  deployment.
