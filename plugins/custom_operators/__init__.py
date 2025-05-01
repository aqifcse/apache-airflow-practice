"""
Custom operators for Apache Airflow.
This module contains custom operator implementations.
"""

from airflow.plugins_manager import AirflowPlugin


class CustomOperatorsPlugin(AirflowPlugin):
    name = "custom_operators_plugin"
    operators = []  # Add your custom operators here
    hooks = []
    executors = []
    macros = []
    admin_views = []
    flask_blueprints = []
    menu_links = []