# gundi-integration-gfw
Gundi Integration for Global Forest Watch (GFW)


## Introduction

This code runs as a Cloud Run Service in Gundi's infrastruction, and can be configured to fetch both VIIRS Fire Alerts and GFW Integrated Alerts.

The configuration requires:

- username and password
- an AOI share link (from your MyGFW Dashboard)

And options include:

- include fire alerts?
- number of days to look for fire data, measured from 'now'.
- lowest confidence level to read for fires (from ['high', 'nominal', 'low'] (default is 'high')
- include GFW Integrated Alerts?
- number of days to look for GFW Integrated Alerts, measured from 'now'.
- lowest confidence level to read for GFW Integrated Alerts (from ['high', 'highest'] (default is 'highest')

This integration's data can be routed to EarthRanger or SMART via SMART Connect.

## Utilities

You aren't required to use this code directly, but it does include a command-line module to make direct queries against the Global Forest Watch API.

Start by setting up your environment with these steps:

### Step 1: clone this repository

```bash
mkdir my-projects && cd my-projects
git clone https://github.com/PADAS/gundi-integration-gfw.git
```

### Step 2: Create a virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -U pip
```

### Step 3: Install dependencies

```bash
pip install -r requirements.txt
```

Now you're all set to use the `cli.py` module in this repo.

### Show help

```bash
python3 cli.py --help
```

you should see:

```bash
Usage: cli.py [OPTIONS] COMMAND [ARGS]...

  A group of commands for getting data from Global Forest Watch API.

Options:
  --help  Show this message and exit.

Commands:
  aoi-info                Fetch AOI Information for the provided URL
  dataset-metadata        Dataset Metadata
  datasets                List Datasets
  geostore-info           Fetch Geostore Information for the provided URL
  get-dataset-fields      Get Dataset Fields
  get-datasets            Get Datasets
  gfw-integrated-alerts   Get GFW Integrated Alerts for the provided URL
  nasa-viirs-fire-alerts  Get NASA Viirs Fire Alerts for the provided URL
```

Each command has it's own arguments. Try running one to see what arguments it requires.


