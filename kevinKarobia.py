import argparse
import logging
import requests
import json

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.options.pipeline_options import StandardOptions
from beam_nuggets.io import relational_db

def run(argv=None):
    """Build and run the pipeline"""
    parser = argparse.ArgumentParser()

    with beam.Pipeline(options=PipelineOptions()) as p:
        sourceConfig = relational_db.SourceConfiguration(
            drivername="postgres+pg8000",
            host="",
            port="",
            username="",
            password="",
            database=""
        )

        databaseData = p | "Reading records from DB" >> relational_db.Read(
            source_config=sourceConfig
        )


