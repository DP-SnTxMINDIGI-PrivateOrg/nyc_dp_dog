# nyc-dog-dataset-dp

Dog Dataset DP POC using Tumult Analytics 

## Tumult analytics

Tumult Analytics 0.16+ requires Python 3.10 through 3.12. Create or select a
Python 3.12 environment before installing the dependencies:

```bash
virtualenv --python=python3.12 .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```
OR 

```bash
pip3 install -r requirements.txt
```

Use Docker:
- Build Docker image
```bash
docker build -t nyc-dog-dp .
```

Run the Docker image:
```bash
docker run --rm \
  -v "$PWD/data/nyc_dog_licenses.csv:/data/nyc_dog_licenses.csv:ro" \
  -v "$PWD/.env:/app/.env:ro" \
  nyc-dog-dp
```