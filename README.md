# db-buildings-composite ![CI](https://github.com/NYCPlanning/db-buildings-composite/workflows/CI/badge.svg)

This repo contains the workflow for creating the Buildings Composite Database.

## Instructions:
1. `./01_dataloading.sh`
2. `./02.1_pad_addr.sh`
3. `./02.2_melissa_addr.sh`
4. `./03_composite.sh`

## About Source Data:
The source data for this workflow consists of:
    - Building footprints maintained by DOITT
    - PLUTO data maintained by DCP
    - NYCEM's initial Buildings Composite dataset, used for building-type classification
    - Property Address Data maintained by DCP
    - Proprietary USPS address information

## DB Location:
Because this product makes use of proprietary USPS address data, the resulting data product is not exported
via GitHub and instead stays on an EDM server.