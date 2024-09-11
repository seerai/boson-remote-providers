import logging
import requests
import os

from typing import List, Union
from datetime import datetime as _datetime
import json
import geopandas as gpd
from cachetools import TTLCache, cached
from shapely import geometry
import urllib.parse


from boson import Pagination
from boson.http import serve
from boson.boson_core_pb2 import Property
from boson.conversion import cql2_to_query_params
from geodesic.cql import CQLFilter
from google.protobuf.timestamp_pb2 import Timestamp

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


STATE_PATH = "/app/states.geoparquet"
COUNTIES_PATH = "/app/counties.geoparquet"


class Boundaries:
    def __init__(self, path: str):
        self.df = gpd.read_parquet(path)

    def intersects(self, geom) -> gpd.GeoDataFrame:
        idx = self.df.intersects(geom)
        return self.df.copy().loc[idx]


counties = Boundaries(COUNTIES_PATH)
states = Boundaries(STATE_PATH)


class NASSQuickStatsRemoteProvider:
    def __init__(self) -> None:
        self.api_url = "https://quickstats.nass.usda.gov/api/api_GET/"
        self.max_page_size = 50000
        # FIXME: take API key out of provider code
        self.api_default_params = {
            "key": "6F441079-980F-3F40-BE4B-F5F17B7ABED3",
        }

    def get_counties_from_geometry(self, geom) -> gpd.GeoDataFrame:
        """
        Given a geometry or bbox, return a geodataframe with 'county_name', 'state_name', 'geometry' (county geometry), sorted by 'COUNTYNS'
        County_name and state_name are the index

        input:
        geom - shapely.geometry or bbox

        output:
        counties_df - geopandas.GeoDataFrame
        """

        if isinstance(geom, list) or isinstance(geom, tuple):
            if len(geom) != 4:
                raise ValueError("bbox must be a bounding box with 4 coordinates")
            geom = geometry.box(*geom)

        elif not isinstance(geom, geometry.base.BaseGeometry):
            raise ValueError("geom must be a shapely geometry or a bbox")

        # get the counties that intersect with the geometry
        counties_df = counties.intersects(geom)
        if len(counties_df) == 0:
            return gpd.GeoDataFrame(columns=["geometry", "id"])

        # get the states that intersect with the geometry, and drop all except the statefp and name
        states_df = states.intersects(geom)
        states_df = states_df[["STATEFP", "NAME"]]

        # strip the whitespace from the statefp
        counties_df["STATEFP"] = counties_df["STATEFP"].str.strip()
        states_df["STATEFP"] = states_df["STATEFP"].str.strip()

        # join the counties and states on the statefp
        counties_df.set_index("STATEFP", inplace=True)
        states_df.set_index("STATEFP", inplace=True)
        counties_and_states = counties_df.join(states_df, rsuffix="_state")

        counties_and_states.rename(
            columns={"NAME": "county_name", "NAME_state": "state_name", "STUSPS": "state_alpha"}, inplace=True
        )
        counties_and_states = counties_and_states.sort_values(by=["COUNTYNS"])

        # Make county_name and state_name uppercase, and set them as the index
        counties_and_states["county_name"] = counties_and_states["county_name"].str.upper()
        counties_and_states["state_name"] = counties_and_states["state_name"].str.upper()
        counties_and_states["state_alpha"] = counties_and_states["state_aplha"].str.upper()

        counties_gdf = counties_and_states.set_index(["county_name", "state_alpha"])
        counties_gdf = counties_gdf[["geometry", "COUNTYNS", "state_name"]]

        # Store the counties_gdf for later use
        self.counties_gdf = counties_gdf
        return

    def get_states_from_geometry(self, geom) -> gpd.GeoDataFrame:
        """
        do this later (for when we can only search by state)
        """
        pass

    def create_query_list(
        self,
        bbox: List[float] = [],
        datetime: List[_datetime] = [],
        intersects: object = None,
        # collections: List[str] = [],
        # feature_ids: List[str] = [],
        filter: Union[CQLFilter, dict] = None,
        # fields: Union[List[str], dict] = None,
        # sortby: dict = None,
        # method: str = "POST",
        # page: int = None,
        # page_size: int = None,
        **kwargs,
    ) -> List[dict]:
        """
        This parses the geodesic search parameters and outputs a list of parameter dicts, one for each state or county and year
        """
        api_params = {}

        """
        DEFAULTS
        """
        if self.api_default_params:
            api_params.update(self.api_default_params)

        """
        BBOX/INTERSECTS::
        bbox must be translated into a list of county names (or state names)
        """
        if bbox:
            logger.info(f"Input bbox: {bbox}")
            geom = geometry.box(*bbox)

        elif intersects:
            logger.info(f"Input intersects: {intersects}")
            geom = intersects

        else:
            logger.info("No bbox or intersects provided. Using US as default.")
            geom = geometry.box(-179.9, 18.0, -66.9, 71.4)

        self.get_counties_from_geometry(geom)
        counties_gdf = self.counties_gdf

        """
        DATETIME: Produce a list of years that intersect with the datetime range 
        """
        if datetime:
            logger.info(f"Received datetime: {datetime}")

            start_year = datetime[0].year
            end_year = datetime[1].year

            years_range = list(range(start_year, end_year + 1))
        else:
            logger.info("No datetime provided. Using 2020-2024 as default.")
            years_range = list(range(2020, 2025))

        """
        FILTER:
        convert cql filter to query parameters and update
        """
        if filter:
            logger.info(f"Received CQL filter")
            filter_params = cql2_to_query_params(filter)

        query_list = []

        for row_index, row in counties_gdf.reset_index().iterrows():
            query_params = {}

            # FIXME: account for the possibility that there is no county (state only)
            query_params["county_name"] = row["county_name"]
            query_params["state_alpha"] = row["state_alpha"]
            query_params["sector_desc"] = "CROPS"

            if filter:
                # FIXME: make sure this doesn't overwrite the other params, and that it consists only of valid params
                query_params.update(filter_params)

            for year_index, year in enumerate(years_range):
                query_params["year"] = year
                query_params["query_index"] = row_index * len(years_range) + year_index
                query_list.append(query_params)

        return query_list

    @cached(cache=TTLCache(maxsize=1024, ttl=3600 * 24))
    def make_request(self, pagination={}, **kwargs) -> gpd.GeoDataFrame:
        """
        Request data from the API and return a GeoDataFrame, and updated pagination object
        """

        # Get the current pagination
        if not pagination:
            pagination = Pagination({"token": "0-10-0"}, 10)
            return gpd.GeoDataFrame(columns=["geometry", "id"]), pagination

        _, _, resource_index = pagination.get_current()

        # Get the parameters for the current resource
        if resource_index >= len(self.query_list):
            logger.info("No more resources to query")
            return gpd.GeoDataFrame(columns=["geometry", "id"]), pagination
        else:
            api_params = self.query_list[resource_index]

        logger.info(f"Making request with params: {api_params}")

        # Make the request
        encoded_params = urllib.parse.urlencode(api_params)
        response = requests.get(f"{self.api_url}?{encoded_params}")

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse and use the response data (JSON in this case)
            res = response.json()

            # Check if the response is empty
            if not res:
                logger.info("No results returned from API")
                gdf = gpd.GeoDataFrame(columns=["geometry", "id"])

            # Get number of results and the geometry from counties_gdf
            n_returned = len(res["data"])
            state_alpha = api_params["state_alpha"]
            county_name = api_params["county_name"]
            area_geometry = self.counties_gdf.loc[(state_alpha, county_name), "geometry"]

            gdf = gpd.GeoDataFrame(data=res["data"], geometry=[area_geometry] * n_returned)
            logger.info(f"Received {n_returned} features")
        else:
            logging.error(f"Error: {response.status_code}")
            gdf = gpd.GeoDataFrame(columns=["geometry", "id"])

        # Update the pagination
        next_pagination = pagination.get_next_token(offset=0, resource_index=resource_index + 1)

        return gdf, next_pagination

    def search(self, pagination={}, provider_properties={}, **kwargs) -> gpd.GeoDataFrame:
        """Implements the Boson Search endpoint."""
        logger.info("Making request to API.")
        logger.info(f"Search received kwargs: {kwargs}")

        """
        PROVIDER_PROPERTIES: 
        """
        if provider_properties:
            logger.info(f"Received provider_properties from boson_config.properties: {provider_properties}")
            # Check for source_desc (Program)
            source_desc = provider_properties.get("source_desc", "SURVEY")
            kwargs["source_desc"] = source_desc

            # Check for statisticcat_desc (Statistic Category)
            statisticcat_desc = provider_properties.get("statisticcat_desc", None)
            if statisticcat_desc:
                kwargs["statisticcat_desc"] = statisticcat_desc

        self.query_list = self.create_query_list(**kwargs)

        """
        PAGINATION and LIMIT
        """
        limit = kwargs.pop("limit", 10)

        if pagination:
            logger.info(f"Received pagination: {pagination}")
        else:
            pagination = Pagination({"token": f"0-{limit}-0"}, limit)

        """
        Make the requests
        """
        gdf, next_pagination = self.make_request(pagination=pagination, **kwargs)

        return gdf, next_pagination

    def queryables(self, **kwargs) -> dict:
        # if you have an openapi file, you can use the get_queryables_from_openapi method
        # to automatically generate the queryables
        if os.path.isfile("path_to_openapi_file"):
            return self.get_queryables_from_openapi(openapi_path="path_to_openapi_file")
        else:
            return {
                "state_alpha": Property(
                    title="state_alpha",
                    type="string",
                ),
                "county_name": Property(
                    title="county_name",
                    type="string",
                ),
                "agg_level_desc": Property(
                    title="aggregation_level",
                    type="string",
                    # enum=["COUNTY", "STATE"],
                ),
                "source_desc": Property(
                    title="source_description",
                    type="string",
                    # enum=["SURVEY", "CENSUS"],
                ),
                "statisticcat_desc": Property(
                    title="statistic_category_description",
                    type="string",
                ),
                "commodity_desc": Property(
                    title="commodity_description",
                    type="string",
                ),
            }


api_wrapper = NASSQuickStatsRemoteProvider()
app = serve(search_func=api_wrapper.search, queryables_func=api_wrapper.queryables)
