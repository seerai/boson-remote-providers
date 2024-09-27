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
from google.protobuf.struct_pb2 import Value
from shapely import geometry


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

# Maximum number of results to return - this is the max that EIA API allows
MAX_PAGE_SIZE = 5000


STATE_PATH = "/app/states.geoparquet"


class Boundaries:
    def __init__(self, path: str):
        self.df = gpd.read_parquet(path)

    def intersects(self, geom) -> gpd.GeoDataFrame:
        idx = self.df.intersects(geom)
        return self.df.copy().loc[idx]


states = Boundaries(STATE_PATH)


class EIAGenerators:
    def __init__(self) -> None:
        self.api_url = "https://api.eia.gov/v2/electricity/operating-generator-capacity/data"
        self.max_page_size = MAX_PAGE_SIZE
        self.api_key = os.getenv("API_KEY")

    def default_params(self):
        return {
            "frequency": "monthly",
            "data": [
                "county",
                "latitude",
                "longitude",
                "nameplate-capacity-mw",
                "net-summer-capacity-mw",
                "net-winter-capacity-mw",
                "operating-year-month",
                "planned-derate-summer-cap-mw",
                "planned-derate-year-month",
                "planned-retirement-year-month",
                "planned-uprate-summer-cap-mw",
                "planned-uprate-year-month",
            ],
            "facets": {},
            "sort": [{"column": "period", "direction": "desc"}],
        }

    def parse_provider_properties(self, provider_properties: dict) -> dict:
        return {}

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

    def get_states_from_geometry(self, geom) -> gpd.GeoDataFrame:
        """
        filter by state
        """
        if isinstance(geom, list) or isinstance(geom, tuple):
            if len(geom) != 4:
                raise ValueError("bbox must be a bounding box with 4 coordinates")
            geom = geometry.box(*geom)

        elif not isinstance(geom, geometry.base.BaseGeometry):
            raise ValueError("geom must be a shapely geometry or a bbox")

        # get the states that intersect with the geometry, and drop all except the statefp and name
        states_df = states.intersects(geom)
        return states_df

    def _get_pagination(self, pagination: dict, limit: int) -> Pagination:
        if pagination:
            return Pagination(pagination, limit)
        return Pagination({}, limit)

    def search(
        self,
        limit: int = 0,
        bbox: list = [],
        intersects: geometry.base.BaseGeometry = None,
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

        x_params = self.default_params()
        x_params.update(
            {
                "offset": offset,
                "length": length,
            }
        )
        x_params.update(self.parse_provider_properties(provider_properties))
        x_params.update(self.parse_datetime(datetime))
        self.update_facets(x_params, filter)

        # If we have a bounding box or a geometry, we need to filter by state
        geom = None
        if bbox:
            geom = geometry.box(*bbox)
        if intersects:
            geom = intersects

        if geom:
            valid = self.update_states(x_params, self.get_states_from_geometry(geom))
            if not valid:
                return gpd.GeoDataFrame(), {}

        # Run the API call
        res = requests.get(
            self.api_url,
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
        if data:
            gdf = gpd.GeoDataFrame(data)
            gdf.loc[:, "period"] = pd.to_datetime(gdf.period, utc=True)
            geometry_column = gpd.points_from_xy(gdf.longitude, gdf.latitude)
            gdf = gpd.GeoDataFrame(gdf, geometry=geometry_column)
        else:
            return gpd.GeoDataFrame(), {}

        return gdf, {"token": pagination.get_next_token(offset + len(gdf))}

    def update_facets(self, x_params: dict, filter: dict):
        """updates the search filters and corresponding api path depending on passed in filters

        https://www.eia.gov/opendata/browser/electricity/operating-generator-capacity
        """
        facets = x_params["facets"]

        # First, convert the filters that are passed in to query parameters
        p = cql2_to_query_params(filter)
        # Filter out any unsupported parameters - these may still be filtered internally in boson, but we
        # only process the ones that are supported by the API
        p = {k: v for k, v in p.items() if k in list(self.queryables()["generators"].keys())}
        for k, v in p.items():
            facets[k] = [x.strip() for x in v.split(",")]

    def update_states(self, x_params: dict, states_df: gpd.GeoDataFrame) -> bool:
        """updates the search filters to use the states that intersect with the geometry"""
        states = x_params["facets"].get("stateid", [])

        states_set = set(states)
        isect = set(states_df.STUSPS)
        if len(states_set) > 0:
            isect = states_set.intersection(isect)

        if isect:
            x_params["facets"]["stateid"] = list(isect)
            return True
        return False

    def queryables(self, **kwargs) -> dict:
        # if you have an openapi file, you can use the get_queryables_from_openapi method
        # to automatically generate the queryables
        return dict(
            generators=dict(
                balancing_authority_code=dict(
                    title="Balancing Authority Code",
                    type="string",
                ),
                energy_source_code=dict(
                    title="Energy Source Code",
                    type="string",
                ),
                entityid=dict(
                    title="Entity ID",
                    type="string",
                ),
                generatorid=dict(
                    title="Generator ID",
                    type="string",
                ),
                plantid=dict(
                    title="Plant ID",
                    type="string",
                ),
                prime_mover_code=dict(
                    title="Prime Mover Code",
                    type="string",
                ),
                sector=dict(
                    title="Sector",
                    type="string",
                ),
                stateid=dict(
                    title="State ID",
                    type="string",
                ),
                status=dict(
                    title="Status",
                    type="string",
                    enum=["OP", "OP", "OS", "SB"],
                ),
                technology=dict(
                    title="Technology",
                    type="string",
                    enum=[
                        "All Other",
                        "Batteries",
                        "Coal Integrated Gasification Combined Cycle",
                        "Conventional Hydroelectric",
                        "Conventional Steam Coal",
                        "Flywheels",
                        "Geothermal",
                        "Hydroelectric Pumped Storage",
                        "Hydrokinetic",
                        "Landfill Gas",
                        "Municipal Solid Waste",
                        "Natural Gas Fired Combined Cycle",
                        "Natural Gas Fired Combustion Turbine",
                        "Natural Gas Internal Combustion Engine",
                        "Natural Gas Steam Turbine",
                        "Natural Gas with Compressed Air Storage",
                        "Nuclear",
                        "Offshore Wind Turbine",
                        "Onshore Wind Turbine",
                        "Other Gases",
                        "Other Natural Gas",
                        "Other Waste Biomass",
                        "Petroleum Coke",
                        "Petroleum Liquids",
                        "Solar Photovoltaic",
                        "Solar Thermal with Energy Storage",
                        "Solar Thermal without Energy Storage",
                        "Wood/Wood Waste Biomass",
                    ],
                ),
                unit=dict(
                    title="Unit",
                    type="string",
                ),
            )
        )


api_wrapper = EIAGenerators()
app = serve(search_func=api_wrapper.search, queryables_func=api_wrapper.queryables)
