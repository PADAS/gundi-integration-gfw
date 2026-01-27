---
name: Decouple Fire and Integrated Alerts Processing
overview: Split the monolithic `get_dataset_and_geostores` function into two separate functions, one for fire alerts and one for integrated alerts, allowing independent processing and scheduling of each feed.
todos:
  - id: todo-1769122135244-s43z1vddt
    content: ""
    status: pending
isProject: false
---

# Decouple Fire and Integrated Alerts Processing

## Current Architecture

The `action_get_dataset_and_geostores` function in [`app/actions/handlers.py`](app/actions/handlers.py) currently processes both feeds in a single function:

- Checks metadata for both fire and integrated alerts datasets
- Uses a single date range calculation (based on `integrated_alerts_lookback_days`)
- Loops through geostores and triggers sub-actions for both feeds
- Saves state for both datasets

## Proposed Changes

### 1. Create Separate Configuration Classes

In [`app/actions/configurations.py`](app/actions/configurations.py):

- Create `GetFireAlertsDatasetAndGeostoresConfig` - contains only fire alerts related config
- Create `GetIntegratedAlertsDatasetAndGeostoresConfig` - contains only integrated alerts related config
- Both will extend `InternalActionConfiguration` and include:
- `integration_id: str`
- `pull_events_config: PullEventsConfig` (for access to shared settings)
- `aoi_data: AOIData`

### 2. Create Separate Action Handlers

In [`app/actions/handlers.py`](app/actions/handlers.py):

**New function: `action_get_fire_alerts_dataset_and_geostores`**

- Handles only fire alerts processing
- Uses `fire_lookback_days` for date range calculation
- Checks fire dataset metadata and state
- Triggers `get_nasa_viirs_fire_alerts_for_geostore_and_date_range` sub-actions
- Saves fire dataset state only

**New function: `action_get_integrated_alerts_dataset_and_geostores`**

- Handles only integrated alerts processing
- Uses `integrated_alerts_lookback_days` for date range calculation
- Checks integrated alerts dataset metadata and state
- Triggers `get_integrated_alerts_for_geostore_and_date_range` sub-actions
- Saves integrated alerts dataset state only

### 3. Update `action_pull_events`

In [`app/actions/handlers.py`](app/actions/handlers.py), modify `action_pull_events` (around line 196-202):

- Replace single `get_dataset_and_geostores` trigger with two separate triggers
- Conditionally trigger fire alerts action if `include_fire_alerts` is True
- Conditionally trigger integrated alerts action if `include_integrated_alerts` is True
- Both triggers can run independently and in parallel

### 4. Remove Old Function

- Remove `action_get_dataset_and_geostores` function
- Remove `GetDatasetAndGeostoresConfig` class (or keep for backward compatibility if needed)

### 5. Update Imports

- Update imports in [`app/actions/handlers.py`](app/actions/handlers.py) to include new config classes
- Remove import of `GetDatasetAndGeostoresConfig` if it's removed

## Benefits

- **Independent Processing**: Each feed can be processed independently without affecting the other
- **Better Error Isolation**: Failures in one feed don't impact the other
- **Flexible Scheduling**: Each feed can potentially have its own schedule in the future
- **Clearer Code**: Each function has a single responsibility
- **Independent Date Ranges**: Each feed uses its own lookback days configuration

## Implementation Details

### Shared Logic Extraction

Both new functions will share similar patterns:

- Dataset metadata retrieval
- State checking and comparison
- Geostore iteration
- Date pair generation
- Sub-action triggering

### State Management

Each function manages its own dataset state:

- Fire alerts: `DATASET_NASA_VIIRS_FIRE_ALERTS`
- Integrated alerts: `DATASET_GFW_INTEGRATED_ALERTS`

### Error Hand