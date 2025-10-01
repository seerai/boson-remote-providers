import logging
import geodesic
import requests
import json
from typing import List, Union, Tuple
from datetime import datetime as _datetime
import traceback
import geopandas as gpd

from boson.http import serve
from boson.conversion import cql2_to_query_params

logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

DEFAULT_FIELDS_TO_RETURN = [
    "id",
    "quality_grade",
    "uuid",
    "license_code",
    "positional_accuracy",
    "public_positional_accuracy",
    "created_at",
    "observed_on",
    "updated_at",
    "species_guess",
    "taxon",
    "geojson",
    "time_observed_at",
    "captive",
    "observation_photos",
    "uri",
    "geoprivacy",
    "location",
    "wikipedia_url",
    "default_photo_url",
    "observation_photo_url",
    "iconic_taxon_name",
    "preferred_common_name",
    "geometry",
]


# Flatten certain properties
def flatten_property(observation, key_structure_list) -> tuple:
    """
    Flatten a nested property in an observation
    input: observation, key_structure_list
    key_structure_list: list of keys that make up the nested property
    """
    flattened_key = "_".join(key_structure_list)
    nested_property = observation.copy()
    for key in key_structure_list:
        nested_property = nested_property.get(key, None)
        if nested_property is None:
            break

    return flattened_key, nested_property


class iNaturalistRemoteProvider:
    def __init__(self) -> None:
        self.api_url = "https://api.inaturalist.org/v1/observations"
        self._max_page_size = 200
        self._required_fields = ["id", "geometry"]
        self._default_fields_to_return = DEFAULT_FIELDS_TO_RETURN

    def parse_provider_properties(self, provider_properties: dict) -> dict:
        """
        Parse provider properties (not implemented)
        """
        return {}

    def parse_input_params(
        self,
        bbox: List[float] = None,
        datetime: List[_datetime] = None,
        intersects: object = None,
        feature_ids: List[str] = None,
        filter: str = None,
        sortby: str = None,
        page: int = None,
        page_size: int = None,
        **kwargs,
    ) -> dict:
        """
        Translate geodesic input parameters to iNaturalist API parameters
        """
        inat_params = {}

        # Add the bbox to the request, if it was provided
        if bbox:
            logger.info(f"Input bbox: {bbox}")
            inat_params["nelat"] = bbox[3]
            inat_params["nelng"] = bbox[2]
            inat_params["swlat"] = bbox[1]
            inat_params["swlng"] = bbox[0]

        # Handle datetimes (inputs are array in datetime format)
        # Datetimes sent to API must be strings in yyyy-mm-dd format
        if datetime:
            logger.info(f"Received datetime: {datetime}")
            startdate = datetime[0]
            enddate = datetime[1]

            if startdate == enddate:
                logger.info(f"Received datetime and start and end datetime are the same: {startdate}")
                inat_params["on"] = startdate.strftime("%Y-%m-%d")
            else:
                logger.info(f"Received datetime range: {startdate} to {enddate}")
                inat_params["d1"] = startdate.strftime("%Y-%m-%d")
                inat_params["d2"] = enddate.strftime("%Y-%m-%d")

        # If a geometry is provided, get the bbox of the geometry and add it to the request
        if intersects:
            logger.info(
                f"Received geometry from intersects keyword. Creating bbox from geometry's bounds: {intersects.bounds}"
            )
            bbox = intersects.bounds
            inat_params["nelat"] = bbox[3]
            inat_params["nelng"] = bbox[2]
            inat_params["swlat"] = bbox[1]
            inat_params["swlng"] = bbox[0]

        # Handle ids
        if feature_ids:
            logger.info(f"Received ids: {feature_ids}")
            inat_params["id"] = feature_ids

        # Handle cql filters
        if filter:
            logger.info(f"Received CQL filter: {filter}")
            inat_params.update(cql2_to_query_params(filter))

        # Handle sorting
        if sortby:
            logger.info(f"Received sortby: {sortby}")
            sortby = sortby[0]
            field = sortby["field"]
            direction = sortby["direction"]
            if field in [
                "created_at",
                "observed_on",
                "species_guess",
                "votes",
                "id",
            ] and direction in ["asc", "desc"]:
                inat_params["order_by"] = field
                inat_params["order"] = direction
            else:
                logger.warning(f"Received invalid sortby, ignoring: {sortby}")

        # Handle pagination
        inat_params["page"] = page
        inat_params["per_page"] = page_size

        return inat_params

    def convert_results_to_features(
        self,
        response: dict,
        fields: Union[List[str], dict] = [],
    ) -> gpd.GeoDataFrame:
        """
        Convert the response from iNaturalist to a list of geodesic.Features
        """
        features = []
        for observation in response["results"]:
            # Extract the coordinates and datetime from the observation
            logger.debug(f"Observation: {observation}")
            geometry = observation.get("geojson", None)
            obs_datetime = observation.get("time_observed_at", None)

            if obs_datetime and obs_datetime != "null":
                logger.debug(f"obs_datetime is not null: {obs_datetime}")
                obs_datetime = _datetime.strptime(obs_datetime, "%Y-%m-%dT%H:%M:%S%z")
            else:
                obs_datetime = observation.get("created_at", None)
                logger.debug(f"obs_datetime is null, using created_at: {obs_datetime}")
                obs_datetime = _datetime.strptime(obs_datetime, "%Y-%m-%dT%H:%M:%S%z")

            if geometry is None:
                geometry = {"type": "Point", "coordinates": [0, 0]}

            feature = {"geometry": geometry, "properties": {}, "datetime": obs_datetime}

            # Flatten certain properties for easier access, and add to the observation dict
            _, wiki = flatten_property(observation, ["taxon", "wikipedia_url"])

            _, default_photo_url = flatten_property(observation, ["taxon", "default_photo", "medium_url"])
            _, taxon_name = flatten_property(observation, ["taxon", "iconic_taxon_name"])
            _, preferred_name = flatten_property(observation, ["taxon", "preferred_common_name"])

            # handle observation photos
            obs_photos = observation.get("observation_photos", [{}])
            if obs_photos:
                obs_photo = obs_photos[0].get("photo", {}).get("url", None)
            else:
                obs_photo = None

            observation["wikipedia_url"] = wiki
            observation["default_photo_url"] = default_photo_url
            observation["observation_photo_url"] = obs_photo
            observation["iconic_taxon_name"] = taxon_name
            observation["preferred_common_name"] = preferred_name

            # Add the observation data to the feature's properties
            feature['properties'].update(observation)

            features.append(feature)
        
        fc = {"type": "FeatureCollection", "features": features}

        logger.debug(f"Number of features: {len(features)}")
        logger.debug(f"type of features: {type(features)}")
        logger.debug(f"features[0]: {features[0] if features else 'No features'}")

        features_gdf = gpd.GeoDataFrame.from_features(fc)

        logger.info(f"shape of features_gdf: {features_gdf.shape}")
        logger.debug(f"features_gdf columns: {features_gdf.columns}")
        logger.debug(f"features_gdf head: {features_gdf.head()}")
        logger.debug(f"Converted features to type: {type(features_gdf)} and shape: {features_gdf.shape}")

        # Deal with fields
        columns_to_return = list(set(self._default_fields_to_return + self._required_fields))
        logger.debug(f"Columns to return before fields parsing: {columns_to_return}")

        if fields:
            logger.info(f"Received fields: {fields}")
            # add columns_to_return from fields['include'], or include all if 'include' is 'all'
            # but first, check if fields is a list or dict. If list, convert to dict with 'include' key
            if isinstance(fields, list):
                fields_dict = {}
                for field in fields:
                    if field.startswith('+'):
                        if 'include' in fields_dict:
                            fields_dict['include'].append(field[1:])
                        else:
                            fields_dict['include'] = [field[1:]]
                    elif field.startswith('-'):
                        if 'exclude' in fields_dict:
                            fields_dict['exclude'].append(field[1:])
                        else:
                            fields_dict['exclude'] = [field[1:]]
                    else:
                        raise ValueError(
                            f"Invalid field format: {field}. Fields should start with '+' or '-'."
                        )
                fields = fields_dict
            
            # The columns logic is based on the dict form of fields. We converted the list forms, so
            # at this point, fields is either a dict or invalid. We check if fields is a dict, and
            # then proceed parsing which columns to include
            if not isinstance(fields, dict):
                raise TypeError(
                    f"""Fields should be a dict with 'include' and/or 'exclude' keys, or 
                    a list of strings each prefixes with '+' or '-'. Received: {type(fields)}"""
                )
            
            if "include" in fields:
                if any(field in ["all", "all_fields", "*"] for field in fields["include"]):
                    columns_to_return = features_gdf.columns
                elif fields["include"]:  # Check if the list is not empty
                    columns_to_return.extend(
                        field
                        for field in fields["include"]
                        if (field not in columns_to_return and field in features_gdf.columns)
                    )

            # remove columns_to_return from fields['exclude']
            if "exclude" in fields:
                if fields["exclude"]:
                    columns_to_return = [
                        field
                        for field in columns_to_return
                        if not (field in fields["exclude"] and field not in self._required_fields)
                    ]

        # Filter the columns to return, keeping in mind that not all columns may be present in the data
        columns_to_return = [col for col in columns_to_return if col in features_gdf.columns]
        logger.debug(f"Columns to return after parsing: {columns_to_return}")

        # Filter the columns to return
        features_gdf = features_gdf[columns_to_return]

        # Make sure the geometry column exists
        if "geometry" not in features_gdf.columns:
            features_gdf["geometry"] = None

        # make sure 'id' is non-null and set as index
        if "id" in features_gdf.columns and features_gdf["id"].notna().all():
            features_gdf = features_gdf.set_index("id")
        else:
            raise ValueError("The 'id' column is missing or contains invalid values.")
        
        logger.debug(
            f"features_gdf has set index to 'id'. features_gdf shape: {features_gdf.shape} vs. number of unique ids {len(features_gdf.index.unique())}"
        )
        logger.debug(f"Filtered fields. Returning gdf ({type(features_gdf)}) with shape: {features_gdf.shape}")

        return features_gdf

    def request_features(self, **kwargs) -> Tuple[gpd.GeoDataFrame, int]:
        """
        Request data from inaturalist and return a GeoDataFrame
        """
        # Translate the input parameters to iNaturalist API parameters
        logger.info(f"Parsing search input parameters: {kwargs}")
        api_params = self.parse_input_params(**kwargs)

        # Make a GET request to the API
        logger.info(f"Making request with params: {api_params}")
        response = requests.get(self.api_url, api_params)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse and use the response data (JSON in this case)
            res = response.json()

            features_gdf = self.convert_results_to_features(res, fields=kwargs.get("fields", {}))

            numberMatched = res["total_results"]
            logger.info(f"Total results (not necessarily returned): {numberMatched}")

            logger.info(f"Received {len(features_gdf)} features")
        else:
            logging.error(f"Error: {response.status_code}")
            numberMatched = 0
            features_gdf = gpd.GeoDataFrame(columns=["geometry", "id"])

        return features_gdf, numberMatched

    def search(self, pagination={}, provider_properties={}, **kwargs) -> geodesic.FeatureCollection:
        """Implements the Boson Search endpoint."""
        logger.info("Making request to iNaturalist API.")

        if provider_properties:
            logger.info(f"Provider properties received: {provider_properties}")
        logger.info(f"Search received kwargs: {kwargs}")

        # Handle pagination, limit and page_size
        page = 1
        page_size = kwargs.get("page_size", None)
        limit = kwargs.get("limit", None)
        if limit == 0:
            limit = 10

        # Establish max_page_size
        max_page_size = provider_properties.get("max_page_size", None)

        if max_page_size:
            logger.info(f"Received max_page_size from provider properties: {max_page_size}")
        else:
            logger.info(f"max_page_size not received from provider properties. Using default: {self._max_page_size}")
            max_page_size = self._max_page_size

        # Set page_size
        if page_size:
            logger.info(f"Received page_size from search: {page_size}")
            if page_size > max_page_size:
                logger.info(f"Page size exceeds max_page_size. Setting page_size to {max_page_size}")
                page_size = max_page_size
        else:
            logger.info(f"page_size not received from search. Using default: {max_page_size}")
            page_size = max_page_size

        if limit:
            logger.info(f"Received limit from search: {limit}")
            if limit < page_size:
                logger.info(f"Limit is smaller than page_size. Setting page_size to {limit}")
                page_size = limit

        # Override page and page_size if pagination is provided
        if pagination:
            logger.info(f"Received pagination: {pagination}")
            page = pagination["page"]
            if page == 0:
                logger.info(f"Received page 0. Setting page to 1")
                page = 1
            page_size = pagination["page_size"]

        # Request the features from the API
        features_gdf, numberMatched = self.request_features(page=page, page_size=page_size, **kwargs)

        if numberMatched < page_size and page != 1:
            logger.info("Preventing duplicate features by returning empty GeoDataFrame")
            features_gdf = gpd.GeoDataFrame(columns=["geometry", "id"])
            pagination_dict = {}

        if len(features_gdf) == 0:
            pagination_dict = {}
        else:
            pagination_dict = {
                "page": page + 1,
                "page_size": page_size,
            }

        logger.debug(f"Search is returning type {type(features_gdf)} and shape {features_gdf.shape}")

        return features_gdf, pagination_dict

    def get_queryables_from_openapi(self, openapi_path: str) -> dict:
        with open(openapi_path, "r") as f:  # loading locally because more speedy
            response = json.load(f)
        queryables = {}

        params = response["parameters"]

        for p in params:
            param = params[p]
            title = param.get("name")
            param_type = param.get("type")

            enum = param.get("enum", None)

            if enum is not None:
                enum = [str(e) for e in enum]
                queryables[title] = dict(title=title, type=param_type, enum=enum)
            else:
                queryables[title] = dict(title=title, type=param_type)

        return queryables

    def queryables(self, **kwargs) -> dict:
        queryables_dict = self.get_queryables_from_openapi("inat_openapi_schema.json")
        return dict(inaturalist=queryables_dict)


inaturalist = iNaturalistRemoteProvider()
app = serve(search_func=inaturalist.search, queryables_func=inaturalist.queryables)
