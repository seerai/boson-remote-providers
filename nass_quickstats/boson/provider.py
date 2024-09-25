import logging
import requests
import os

from typing import List, Union
from datetime import datetime as _datetime
import geopandas as gpd
from cachetools import TTLCache, cached
from shapely import geometry
import urllib.parse
import pandas as pd

from boson import Pagination
from boson.http import serve
from boson.conversion import cql2_to_query_params

from geodesic.cql import CQLFilter

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


class NASSQuickStats:
    def __init__(self) -> None:
        self.api_url = "https://quickstats.nass.usda.gov/api/api_GET/"
        self.max_page_size = 50000
        self.api_default_params = {"key": os.getenv("API_KEY")}

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
        counties_gdf = counties.intersects(geom)
        if len(counties_gdf) == 0:
            return gpd.GeoDataFrame(columns=["geometry", "id"])

        return counties_gdf

    def get_states_from_geometry(self, geom) -> gpd.GeoDataFrame:
        """
        do this later (for when we can only search by state)
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

    def create_query_list(
        self,
        bbox: List[float] = [],
        datetime: List[_datetime] = [],
        intersects: object = None,
        filter: Union[CQLFilter, dict] = None,
        extra_params: dict = {},
        **kwargs,
    ) -> List[dict]:
        """parses the geodesic search parameters into a list of query parameters for the API"""

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

        counties_gdf = self.get_counties_from_geometry(geom)
        states_gdf = self.get_states_from_geometry(geom)

        """
        DATETIME: Produce a list of years that intersect with the datetime range 
        """
        if datetime:
            logger.info(f"Received datetime: {datetime}")

            start_year = datetime[0].year
            end_year = datetime[1].year

            years_range = list(range(start_year, end_year + 1))
        else:
            logger.info("No datetime provided. Using 2023 as default.")
            years_range = [2023]

        """
        FILTER:
        convert cql filter to query parameters and update
        """
        if filter:
            logger.info("Received CQL filter")
            filter_params = cql2_to_query_params(filter)

        query_list = []

        for row_index, row in states_gdf.reset_index().iterrows():
            query_params = {}

            query_params.update(extra_params)

            if self.api_default_params:
                query_params.update(self.api_default_params)

            # FIXME: account for the possibility that there is no county (state only)
            query_params["state_fips_code"] = row["STATEFP"]
            query_params["sector_desc"] = "CROPS"
            query_params["agg_level_desc"] = "COUNTY"

            if filter:
                # FIXME: make sure this doesn't overwrite the other params, and that it consists only of valid params
                query_params.update(filter_params)

            for year_index, year in enumerate(years_range):
                query_params["year"] = year
                # query_params["query_index"] = row_index * len(years_range) + year_index
                query_list.append(query_params)

        return query_list, counties_gdf

    @cached(cache=TTLCache(maxsize=1024, ttl=3600 * 24))
    def _make_request(self, encoded_params: str) -> pd.DataFrame:
        response = requests.get(f"{self.api_url}?{encoded_params}")

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse and use the response data (JSON in this case)
            res = response.json()

            # Check if the response is empty
            if not res:
                logger.info("No results returned from API")
                df = pd.DataFrame()

            # Get number of results and the geometry from counties_gdf
            n_returned = len(res["data"])
            logger.info(f"Received {n_returned} features")
            df = pd.DataFrame(res["data"])
        else:
            logging.error(f"Error: {response.status_code}")
            df = pd.DataFrame()

        return df

    def make_request(
        self, pagination={}, query_list=[], counties_gdf=None, **kwargs
    ) -> gpd.GeoDataFrame:
        """
        Request data from the API and return a GeoDataFrame, and updated pagination object
        """
        offset, page_size, resource_index = pagination.get_current()

        logger.info(
            f"Current pagination: offset={offset}, page_size={page_size}, resource_index={resource_index}"
        )
        logger.info(f"Current query_list len: {len(query_list)}")
        # Get the parameters for the current resource
        if resource_index >= len(query_list):
            logger.info("No more resources to query")
            return gpd.GeoDataFrame(columns=["geometry", "id"]), {}

        results_gdf = gpd.GeoDataFrame(columns=["geometry", "id"])

        for resource_index in range(resource_index, len(query_list)):

            api_params = query_list[resource_index]
            logger.info(f"Making request with params: {api_params}")

            # Make the request
            encoded_params = urllib.parse.urlencode(api_params)
            df = self._make_request(encoded_params)
            logger.info(f"len(df) from _make_request: {len(df)}")

            joined_df = pd.merge(
                df,
                counties_gdf,
                left_on=["state_fips_code", "county_code"],
                right_on=["STATEFP", "COUNTYFP"],
                how="inner",
            )
            gdf = gpd.GeoDataFrame(joined_df, geometry=joined_df.geometry)

            # Append the results to the results_gdf
            end_index = offset + min(len(gdf), page_size - len(results_gdf))
            logger.info(f"end_index: {end_index}")
            gdf = gdf[offset:end_index]

            logger.info(f"Appending {len(gdf)} results to the results_gdf")
            logger.info(
                f"offset: {offset}, page_size: {page_size}, len(results_gdf): {len(results_gdf)}"
            )
            offset = end_index

            temp = pd.concat([results_gdf, gdf], ignore_index=True)
            results_gdf = gpd.GeoDataFrame(temp, geometry=temp.geometry)

            if len(results_gdf) >= page_size:
                results_gdf = results_gdf[:page_size]
                break

        # Update the pagination
        next_pagination = pagination.get_next_token(offset=offset, resource_index=resource_index)

        return results_gdf, next_pagination

    def search(self, pagination={}, provider_properties={}, **kwargs) -> gpd.GeoDataFrame:
        """Implements the Boson Search endpoint."""
        logger.info("Making request to API.")
        logger.info(f"Search received kwargs: {kwargs}")

        """
        PROVIDER_PROPERTIES
        """
        extra_params = {}

        logger.info(
            f"Received provider_properties from boson_config.properties: {provider_properties}"
        )

        # Check for source_desc (Program)
        source_desc = provider_properties.get("source_desc", "SURVEY")
        extra_params["source_desc"] = source_desc

        # Check for statisticcat_desc (Statistic Category)
        statisticcat_desc = provider_properties.get("statisticcat_desc", None)
        if statisticcat_desc:
            extra_params["statisticcat_desc"] = statisticcat_desc

        # Check for commodity_desc (Commodity)
        commodity_desc = provider_properties.get("commodity_desc", "CORN")
        if commodity_desc:
            extra_params["commodity_desc"] = commodity_desc

        query_list, counties_gdf = self.create_query_list(extra_params=extra_params, **kwargs)

        """
        PAGINATION and LIMIT
        """
        limit = kwargs.pop("limit", 10)
        if limit == 0:
            limit = 10

        pagination = Pagination(pagination, limit)
        logger.info(f"limit: {limit}, pagination page_size: {pagination.page_size}")

        """
        Make the requests
        """
        gdf, next_pagination = self.make_request(
            pagination=pagination, query_list=query_list, counties_gdf=counties_gdf, **kwargs
        )

        return gdf, next_pagination

    def queryables(self, **kwargs) -> dict:
        # if you have an openapi file, you can use the get_queryables_from_openapi method
        # to automatically generate the queryables
        return dict(
            commodities=dict(
                state_alpha=dict(
                    title="state_alpha",
                    type="string",
                ),
                county_name=dict(
                    title="county_name",
                    type="string",
                ),
                agg_level_desc=dict(
                    title="agg_level_desc",
                    type="string",
                    enum=["COUNTY", "STATE"],
                ),
                source_desc=dict(
                    title="source_desc",
                    type="string",
                    enum=["SURVEY", "CENSUS"],
                ),
                statisticcat_desc=dict(
                    title="statisticcat_desc",
                    type="string",
                ),
                commodity_desc=dict(
                    title="commodity_desc",
                    type="string",
                ),
            )
        )


api_wrapper = NASSQuickStats()
app = serve(search_func=api_wrapper.search, queryables_func=api_wrapper.queryables)
