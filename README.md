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

