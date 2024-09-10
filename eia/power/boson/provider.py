import os
import logging
import requests
import json
from datetime import datetime as datetime_
from typing import List

import pandas as pd
import geopandas as gpd

from boson import Pagination
from boson.conversion import cql2_to_query_params
from boson.http import serve

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

# Maximum number of results to return - this is the max that EIA API allows
MAX_PAGE_SIZE = 5000


class EIAElectricity:
    def __init__(self) -> None:
        self.api_url = "https://api.eia.gov/v2/electricity/rto/{api}/data"
        self.max_page_size = MAX_PAGE_SIZE
        self.api_key = os.getenv("API_KEY")
        self.default_frequency = "daily"

    def parse_provider_properties(self, provider_properties: dict) -> dict:
        frequency = provider_properties.get("frequency", self.default_frequency)
        if frequency not in ["hourly", "daily"]:
            raise ValueError(f"Invalid frequency: {frequency}")

        api = "daily-region-data"
        if frequency == "hourly":
            api = "region-data"

        fueltype = provider_properties.get("fueltype", [])
        if fueltype:
            api = "fuel-type-data"
            if frequency == "daily":
                api = "daily-fuel-type-data"

            params = {
                "frequency": frequency,
                "api": api,
                "facets": {"fueltype": fueltype},
            }
            if frequency == "daily":
                params["facets"]["timezone"] = provider_properties.get("timezone", ["Eastern"])
            return params

        params = {
            "frequency": provider_properties.get("frequency", self.default_frequency),
            "api": api,
            "facets": {"type": [provider_properties.get("metric", "D")], "timezone": ["Eastern"]},
        }

        if frequency == "hourly":
            params["facets"].pop("timezone", None)

        return params

    def parse_datetime(self, datetimes: List[datetime_]) -> dict:
        """Converts a list of Timestamps to a dictionary of start and end datetimes."""
        if not datetimes:
            return {
                "start": None,
                "end": None,
            }
        return {
            "start": datetimes[0].strftime("%Y-%m-%d"),
            "end": datetimes[1].strftime("%Y-%m-%d"),
        }

    def _get_pagination(self, pagination: dict, limit: int) -> Pagination:
        if pagination:
            return Pagination(pagination, limit)
        return Pagination({}, limit)

    def search(
        self,
        limit: int = 0,
        pagination: dict = {},
        provider_properties: dict = {},
        filter: dict = {},
        count_only: bool = False,
        datetime: list = [],
        **kwargs,
    ) -> gpd.GeoDataFrame:
        """Implements the Boson Search endpoint."""

        if limit == 0:
            limit = self.max_page_size

        # We can only request a max of 5000 records at a time, this is to
        # keep our place when iterating through results
        pagination = self._get_pagination(pagination, limit)
        offset, length, _ = pagination.get_current()

        # If we just need the total results, we can just return that
        if count_only:
            length = 0

        x_params = {
            "data": ["value"],
            "offset": offset,
            "length": length,
            "sort": [{"column": "period", "direction": "desc"}],
        }
        x_params.update(self.parse_provider_properties(provider_properties))
        x_params.update(self.parse_datetime(datetime))

        self.update_facets(x_params, filter)

        url = self.api_url.format(api=x_params.pop("api"))
        res = requests.get(
            url,
            params={"api_key": self.api_key},
            headers={"X-Params": json.dumps(x_params)},
        )

        js = res.json()
        if "error" in js:
            raise ValueError(js["error"])

        response = js.get("response")
        if count_only:
            return int(response.get("total", 0))

        # Create the dataframe from the results
        data = response.get("data", [])
        gdf = gpd.GeoDataFrame(data)
        gdf.loc[:, "period"] = pd.to_datetime(gdf.period, utc=True)

        return gdf, {"token": pagination.get_next_token(offset + len(gdf))}

    def update_facets(self, x_params: dict, filter: dict):
        """updates the search filters and corresponding api path depending on passed in filters

        https://www.eia.gov/opendata/browser/electricity/rto
        """
        facets = x_params["facets"]

        # First, convert the filters that are passed in to query parameters
        p = cql2_to_query_params(filter)
        # Filter out any unsupported parameters - these may still be filtered internally in boson, but we
        # only process the ones that are supported by the API
        p = {k: v for k, v in p.items() if k in ["timezone", "respondent", "subba", "fueltype"]}
        for k, v in p.items():
            facets[k] = [x.strip() for x in v.split(",")]

        # For hourly data, we need to remove the timezone facet - hourly is always in UTC
        if x_params["frequency"] == "hourly":
            facets.pop("timezome", None)

    def queryables(self, **kwargs) -> dict:
        # if you have an openapi file, you can use the get_queryables_from_openapi method
        # to automatically generate the queryables
        return dict(
            electricity=dict(
                respondent=dict(
                    title="Balancing Authority",
                    type="string",
                ),
                subba=dict(
                    title="Subregion",
                    type="string",
                ),
                timezone=dict(
                    title="Timezone",
                    type="string",
                    enum=["Arizona", "Central", "Eastern", "Mountain", "Pacific"],
                ),
                fueltype=dict(
                    title="Energy Source",
                    type="string",
                    enum=["COL", "NG", "NUC", "OIL", "OTH", "SUN", "WND", "WAT", "UNK"],
                ),
            )
        )


api_wrapper = EIAElectricity()
app = serve(search_func=api_wrapper.search, queryables_func=api_wrapper.queryables)
