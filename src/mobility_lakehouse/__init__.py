"""Locally executed TLC source-version pipeline; Azure deployment is a separate stage."""
from .pipeline import ContractError, fetch, ingest, report

__all__ = ["ContractError", "fetch", "ingest", "report"]
