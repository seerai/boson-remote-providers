import logging
import requests
import os

from typing import List, Union
from datetime import datetime as _datetime
import json
import geopandas as gpd

from boson.http import serve
from boson.boson_core_pb2 import Property
from boson.conversion import cql2_to_query_params
from geodesic.cql import CQLFilter
from google.protobuf.timestamp_pb2 import Timestamp

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)


class Boundaries:
    def __init__(self, path: str):
        self.df = gpd.read_parquet(path)

    def intersects(self, geom) -> gpd.GeoDataFrame:
        idx = self.df.intersects(geom)
        return self.df.copy().loc[idx]


class APIWrapperRemoteProvider:
    def __init__(self) -> None:
        self.api_url = "api endpoint url goes here"
        self.max_page_size = 200
        self.api_default_params = {
            # TODO: Add default parameters to the request
            "api_key": "api key goes here",
            "format": "json",
        }

    def parse_input_params(
        self,
        bbox: List[float] = [],
        datetime: List[_datetime] = [],
        intersects: object = None,
        collections: List[str] = [],
        feature_ids: List[str] = [],
        filter: Union[CQLFilter, dict] = None,
        fields: Union[List[str], dict] = None,
        sortby: dict = None,
        method: str = "POST",
        page: int = None,
        page_size: int = None,
        **kwargs,
    ) -> dict:
        """
        Translate geodesic input parameters to API parameters. This function accepts the boson search function
        parameters and returns a dictionary (api_params) with the parameters to be used in the API request.
        """
        api_params = {}

        """
        DEFAULTS: Add default parameters to the request. TODO: Edit these in the __init__ method.
        """
        if self.api_default_params:
            api_params.update(self.api_default_params)

        """
        BBOX: Add the bbox to the request, if it was provided
        """
        if bbox:
            logger.info(f"Input bbox: {bbox}")
            api_params["bbox"] = bbox
        else:
            logger.info("No bbox provided")

        """
        DATETIME: datetimes are provided as a list of two timestamps. TODO: Convert to whatever the API expects
        """
        if datetime:
            logger.info(f"Received datetime: {datetime}")

            # Example of how to handle datetime for an API that expect startdate and enddate in YYYY-MM-DD format
            api_params["startdate"] = datetime[0].strftime("%Y-%m-%d")
            api_params["enddate"] = datetime[1].strftime("%Y-%m-%d")

        """
        INTERSECTS: Handle provided geometry. Unless the API accepts a geometry, this will be difficult to implement.
        In this example, we replace the bbox parameter with the bounding box of the geometry. This will provide
        some preliminary filtering, and then the results could be further filtered to fit the geometry after the 
        features are returned.
        """
        if intersects:
            logger.info(f"Received geometry from intersects keyword with bounds: {intersects.bounds}")
            # Example: take the bounds of the geometry and use as bbox
            bbox = intersects.bounds
            api_params["bbox"] = bbox

        """ 
        COLLECTIONS: Handle collections, if applicable. Not implemented in this example.
        """
        if collections:
            logger.info(f"Received collections: {collections}")
            logger.info("Collections are not implemented here")

        """
        IDS: Handle ids
        """
        if feature_ids:
            logger.info(f"Received ids of length: {len(feature_ids)}")
            api_params["ids"] = feature_ids  # TODO: Update the key to match the API

        """
        FILTER: Handle CQL2 filters. The cql2_to_query_params function will convert the CQL2 filter to a dictionary
            for cql filters with the "logical_and" and "eq" operators. The CQL filters are the way to pass api parameters to the
            search function.
        """
        if filter:
            logger.info(f"Received CQL filter")
            api_params.update(cql2_to_query_params(filter))

        """
        FIELDS:  list of fields to include/exclude. Included fields should be prefixed by 
        "+" and excluded fields by "-". Alernatively, a dict with a "include"/"exclude" lists 
        may be provided
        """
        if fields:
            logger.info(f"Received fields: {fields}")
            if isinstance(fields, dict):
                include = fields.get("include", [])
                exclude = fields.get("exclude", [])
            else:
                include = [field for field in fields if field[0] == "+"]
                exclude = [field for field in fields if field[0] == "-"]
            # Example API has only exclude parameter TODO: Edit this to fit the API
            api_params["exclude_columns"] = exclude

        """
        SORTBY: Handle sorting. Sortby is a dict containing “field” and “direction”. 
        Direction may be one of “asc” or “desc”. Not supported by all datasets
        """
        if sortby:
            logger.info(f"Received sortby: {sortby}")
            api_params["sort"] = sortby.get("direction", "asc")

        """
        METHOD: Handle the method. This is the HTTP method to use for the request.
        """
        if "method" in self.queryables():
            logger.info(f"Received method: {method}")
            api_params["method"] = method

        """
        PAGINATION: Handle pagination (page and page_size)
        """
        if "page" in self.queryables():
            api_params["page"] = page
        if "page_size" in self.queryables():
            api_params["page_size"] = page_size

        return api_params

    def convert_results_to_gdf(self, response: Union[dict, List[dict]]) -> gpd.GeoDataFrame:
        """
        Convert the response from the API to a GeoDataFrame. We are assuming the response is a list of json/dict.
        You may need to get the "results" key from the response, depending on the API.

        The template assumes point features and a single datetime, but this can be modified to handle other geometries
        and multiple datetimes. The remaining outputs from the API response can be added to the properties dictionary.
        """

        # This may need editing, depending on the API response
        if isinstance(response, dict):
            response = response.get("results", [])

        logger.info("Converting API response to GeoDataFrame.")
        logger.info(f"Received {len(response)} results. Converting to GeoDataFrame.")
        if len(response) == 0:
            return gpd.GeoDataFrame(columns=["geometry", "id"])

        logger.info(f"First result: {response[0]}")

        # TODO: Update the keys to match the API response
        LATIDUDE_KEY = "Latitude"
        LONGITUDE_KEY = "Longitude"
        ID_KEY = "id"
        DATETIME_KEY = "UTC"

        gdf = gpd.GeoDataFrame(
            response,
            geometry=gpd.points_from_xy(
                [obs.get(LONGITUDE_KEY) for obs in response],
                [obs.get(LATIDUDE_KEY) for obs in response],
            ),
        )

        gdf.set_index(ID_KEY, inplace=True)

        # TODO: update datetime format
        gdf["datetime"] = gdf[DATETIME_KEY].apply(lambda x: _datetime.strptime(x, format="%Y-%m-%dT%H:%M")).astype(str)

        return gdf

    def request_features(self, **kwargs) -> gpd.GeoDataFrame:
        """
        Request data from the API and return a GeoDataFrame. This function is unlikely to need
        modification.
        """
        # Translate the input parameters to API parameters
        logger.info(f"Parsing search input parameters: {kwargs}")
        api_params = self.parse_input_params(**kwargs)

        # Make a GET request to the API
        logger.info(f"Making request with params: {api_params}")
        response = requests.get(self.api_url, api_params)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse and use the response data (JSON in this case)
            res = response.json()

            # Check if the response is empty
            if not res:
                logger.info("No results returned from API")
                gdf = gpd.GeoDataFrame(columns=["geometry", "id"])

            gdf = self.convert_results_to_gdf(res)
            logger.info(f"Received {len(gdf)} features")
        else:
            logging.error(f"Error: {response.status_code}")
            gdf = gpd.GeoDataFrame(columns=["geometry", "id"])

        return gdf

    def search(self, pagination={}, provider_properties={}, **kwargs) -> gpd.GeoDataFrame:
        """Implements the Boson Search endpoint."""
        logger.info("Making request to API.")
        logger.info(f"Search received kwargs: {kwargs}")

        """
        PAGINATION and LIMIT: if limit is None, Boson will page through all results. Set a max
        page size in the __init__ to control the size of each page. If limit is set, the search function
        will return that number of results. Pagination is a dictionary with the keys "page" and "page_size".
        We will pass "page" and "page_size" to the request_features function.
        """
        page = 1
        page_size = self.max_page_size
        limit = kwargs.get("limit", None)
        if limit == 0:
            limit = None
        if limit is not None:
            page_size = limit if limit <= self.max_page_size else self.max_page_size

        if pagination:
            logger.info(f"Received pagination: {pagination}")
            page = pagination.get("page", None)
            page_size = pagination.get("page_size", self.max_page_size)

        """
        PROVIDER_PROPERTIES: These are the properties set in the boson_config.properties. These are an
        advanced feature and may not be needed for most providers. 
        """
        if provider_properties:
            logger.info(f"Received provider_properties from boson_config.properties: {provider_properties}")
            # Check for source_desc (Program)

        gdf = self.request_features(page=page, page_size=page_size, **kwargs)

        return gdf, {
            "page": page + 1,
            "page_size": page_size,
        }

    def get_queryables_from_openapi(self, openapi_path: str) -> dict:
        """
        This method is used to automatically generate the queryables from an openapi file. Manually entering the
        queryyables is laborious. If the external API provides and OpenAPI spec, this method will read it from
        a json file and return the queryables automatically. (credit: Mark Schulist)
        """
        with open(openapi_path, "r") as f:  # loading locally because more speedy
            response = json.load(f)
        queryables = {}

        path = "/occurrence/search"  # TODO: Update with path for your API

        params = response["paths"][path]["get"]["parameters"]

        for param in params:
            title = param.get("name")
            type = param.get("type")
            enum = None
            if param.get("schema") is not None:
                schema = param.get("schema")
                if schema.get("items") is not None:
                    items = schema.get("items")
                    enum = items.get("enum")
            if enum is not None:
                queryables[title] = Property(title=title, type=type, enum=enum)
            else:
                queryables[title] = Property(title=title, type=type)

        return queryables

    def queryables(self, **kwargs) -> dict:
        """
        Update this method to return a dictionary of queryable parameters that the API accepts.
        The keys should be the parameter names. The values should be a Property object that follows
        the conventions of JSON Schema.
        """
        # if you have an openapi file, you can use the get_queryables_from_openapi method
        # to automatically generate the queryables
        if os.path.isfile("path_to_openapi_file"):
            return self.get_queryables_from_openapi(openapi_path="path_to_openapi_file")
        else:
            return {
                "example_parameter": Property(
                    title="parameter_title",
                    type="string",
                    enum=[
                        "option1",
                        "option2",
                        "option3",
                    ],
                ),
                "example_parameter2": Property(
                    title="parameter_title2",
                    type="integer",
                ),
                "example_parameter3": Property(
                    title="parameter_title3",
                    type="integer",
                ),
                "example_parameter4": Property(
                    title="parameter_title4",
                    type="boolean",
                ),
            }


api_wrapper = APIWrapperRemoteProvider()
app = serve(search_func=api_wrapper.search, queryables_func=api_wrapper.queryables)
