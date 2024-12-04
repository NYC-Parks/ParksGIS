from arcgis._impl.common._filters import GeometryFilter
from arcgis.features import (
    FeatureLayer,
    FeatureSet,
    GeoAccessor,
    Table,
    FeatureLayerCollection,
)
from arcgis.geometry import Geometry, SpatialReference
from arcgis.gis import GIS, Item
from ast import List
from datetime import datetime
import json
from numpy import ndarray
from pandas import DataFrame, Series, concat
import requests
from typing import Any, Literal, Optional, Union
from urllib.parse import urlparse
from uu import Error
from uuid import UUID

Map = dict[
    str,
    dict[
        Literal["Value", "Source"],
        Union[str, int, None],
    ],
]


class Map_Util:
    @staticmethod
    def getValue(
        key: str,
        map: Map,
        source: DataFrame,
    ) -> Any:
        keys = list(map[key].keys())
        if len(keys) != 1:
            raise Error(f"Zero or more than one key found: {keys}")

        if keys[0] == "Value":
            value = map[key].get("Value", None)

            if isinstance(value, str) and value == "utcNow":
                value = datetime.utcnow()

        else:
            column = map[key].get("Source", None)

            if column is None:
                raise Exception(f"Invalid column {key}.")

            if isinstance(column, str) and column.count("+") > 0:
                columns = [col.strip() for col in column.split("+")]
                value = source[columns].agg(lambda x: "".join(x.map(str)), 1)
            else:
                value = source[column].values

        return value


class DF_Util:
    @staticmethod
    def createFromDF(
        source: DataFrame,
        map: Map,
        x: Optional[str] = None,
        y: Optional[str] = None,
    ) -> DataFrame:
        df = DataFrame()

        rows = source.shape[0]
        for col in map:
            value = Map_Util.getValue(
                col,
                map,
                source,
            )

            if (
                isinstance(value, List)
                or isinstance(value, ndarray)
                or isinstance(value, Series)
            ):
                df[col] = value
            else:
                df[col] = [value] * rows

        if x is not None and y is not None:
            df = GeoAccessor.from_xy(df, x, y, sr=2263)

        return df

    @staticmethod
    def createFromList(list: list) -> DataFrame:
        if isinstance(list[0], dict):
            return DataFrame(list)
        return DataFrame([i.__dict__ for i in list])

    @staticmethod
    def update(
        destination: DataFrame,
        destinationKey: str,
        source: DataFrame,
        sourceKey: str,
        map: Map,
    ) -> DataFrame:
        rekey = False
        columnValue = destination[destinationKey][0]

        # create temp key for unhashable arrays
        if isinstance(columnValue, list) or isinstance(columnValue, ndarray):
            rekey = True

            destination[destinationKey + "source"] = destination[destinationKey].apply(
                lambda x: "".join(str(i) for i in x)
            )
            destinationKey += "source"

            source[sourceKey + "delta"] = source[sourceKey].apply(
                lambda x: "".join(str(i) for i in x)
            )
            sourceKey += "delta"

        # merge dataframes on keys
        merged = destination.merge(
            source,
            left_on=destinationKey,
            right_on=sourceKey,
            how="right",
            suffixes=(None, "_right"),
            indicator=True,
        )

        # map delta columns to source columns
        for col in map:
            values = Map_Util.getValue(
                col,
                map,
                merged,
            )

            merged[col] = values

        # remove all added columns
        columns = source.columns.to_list()
        columns.append("_merge")
        if rekey:
            columns.append(destinationKey)
            columns.append(sourceKey)

        removed = merged.drop(columns=columns)

        rename = {}
        for col in removed.columns:
            if "_right" in col:
                rename[col] = col.replace("_right", "")

        return removed.rename(columns=rename)


spatialRef = SpatialReference({"wkid": 102718, "latestWkid": 2263})


# There is a bug in argcis.features.FeatureLayerCollection.query()
# layer_defs_filter is not being added to query param
class FeatureLayerCollectionDecorator:
    _featureLayerCollection: FeatureLayerCollection
    _token: str

    def __init__(
        self, token: str, collection_or_item: Union[FeatureLayerCollection, Item]
    ) -> None:
        self._token = token
        if isinstance(collection_or_item, FeatureLayerCollection):
            self._featureLayerCollection = collection_or_item
        else:
            self._featureLayerCollection = FeatureLayerCollection.fromitem(
                collection_or_item
            )

    def query(
        self,
        layer_defs_filter: list[dict[str, Any]],
        geometry_filter: Optional[GeometryFilter] = None,
        time_filter=None,
        return_geometry=True,
        return_ids_only=False,
        return_count_only=False,
        return_z=False,
        return_m=False,
        out_sr=None,
        as_df=True,
    ) -> DataFrame:
        response = requests.post(
            self._featureLayerCollection.url + "/query",
            {
                "layerDefs": str(layer_defs_filter),
                "geometry": str(geometry_filter["geometry"]),
                "geometryType": geometry_filter["geometryType"],
                "spatialRel": geometry_filter["spatialRel"],
                "returnDistinctValues": False,
                "returnGeometry": return_geometry,
                "returnIdsOnly": return_ids_only,
                "returnCountOnly": return_count_only,
                "returnZ": return_z,
                "returnM": return_m,
                "multipatchOption": "xyFootprint",
                "returnTrueCurves": False,
                "sqlFormat": "none",
                "outSR": out_sr,
                "time": time_filter,
                "f": "pjson",
                "token": self._token,
            },
        ).json()

        if response.get("error", None) != None:
            raise Error(response["error"])

        # TODO: MUST REFACTOR!!! SOMEONELESE NEEDS TO FIX THIS HOT MESS!!
        if as_df:
            df = DataFrame({"Point": geometry_filter["geometry"]["points"]})

            for layer in response["layers"]:
                fields = layer.get("fields", None)
                features = layer.get("features", None)

                if fields is not None:
                    for field in fields:

                        if len(features) == 1:
                            df[field["name"]] = next(
                                iter(features[0]["attributes"].values())
                            )

                        else:
                            df[field["name"]] = None

                            for i in df.index:
                                for feature in features:
                                    geometry = feature["geometry"]
                                    geometry["spatialReference"] = spatialRef
                                    ploygon = Geometry(geometry)

                                    xy = df.at[i, "Point"]
                                    point = Geometry(
                                        {
                                            "x": xy[0],
                                            "y": xy[1],
                                            "spatialReference": spatialRef,
                                        }
                                    )

                                    if ploygon.intersect(point):
                                        colName = field["name"]
                                        value = feature["attributes"][colName]
                                        df.at[i, colName] = value
            return df

        else:
            return response


class LayerServerGen:
    id: int
    serverGen: int  # EPOCH time to start from in milliseconds

    def __init__(self, id: int, serverGen: int) -> None:
        self.id = id
        self.serverGen = serverGen


class LayerDomainNames:
    id: int
    names: list[str]

    def __init__(self, id: int, names: list[str]) -> None:
        self.id = id
        self.names = names


class Server:
    _collection: FeatureLayerCollectionDecorator

    def __init__(self, collection: FeatureLayerCollectionDecorator) -> None:
        self._collection = collection

    def extractChanges(
        self,
        layer_servergen: list[LayerServerGen],
        inserts: bool = True,
        updates: bool = True,
        deletes: bool = False,
    ):
        return self._collection._featureLayerCollection.extract_changes(
            layers=[i.id for i in layer_servergen],
            layer_servergen=[i.__dict__ for i in layer_servergen],
            return_inserts=inserts,
            return_updates=updates,
            return_deletes=deletes,
            return_ids_only=True,
        )

    def query(
        self,
        layerDefinitions: list[
            dict[
                Literal["layerId", "where", "outFields"],
                Union[int, str],
            ],
        ],
        geometry: Optional[
            Union[
                dict[
                    Literal["x", "y"],
                    float,
                ],
                dict[
                    Literal["points"],
                    Union[ndarray, list],
                ],
            ]
        ] = None,
        geometryType: Literal[
            "esriGeometryPoint",
            "esriGeometryMultipoint",
            "esriGeometryPolyline",
            "esriGeometryPolygon",
            "esriGeometryEnvelope",
            "esriGeometryMultiPatch",
        ] = "esriGeometryPoint",
        spatialRel: Literal[
            "esriSpatialRelIntersects",
            "esriSpatialRelContains",
            "esriSpatialRelCrosses",
            "esriSpatialRelEnvelopeIntersects",
            "esriSpatialRelIndexIntersects",
            "esriSpatialRelOverlaps",
            "esriSpatialRelTouches",
            "esriSpatialRelWithin",
        ] = "esriSpatialRelIntersects",
        return_geometry=False,
        as_df=True,
    ):
        if geometry is not None:
            if list(geometry.keys()).count("points") != 0:
                if isinstance(geometry["points"], ndarray):
                    geometry["points"] = geometry["points"].tolist()

                geometry["geometry"] = {"points": geometry["points"]}
                del geometry["points"]

            geometry["spatialReference"] = spatialRef  # type: ignore
            geometry["geometryType"] = geometryType  # type: ignore
            geometry["spatialRel"] = spatialRel  # type: ignore

        return self._collection.query(
            layer_defs_filter=layerDefinitions,
            geometry_filter=geometry,
            return_geometry=return_geometry,
            as_df=as_df,
        )

    def queryDomains(
        self,
        layer_domainnames: list[LayerDomainNames],
    ):
        domains = self._collection._featureLayerCollection.query_domains(
            [l.id for l in layer_domainnames]
        )
        nameSet = {item for l in layer_domainnames for item in l.names}
        return [d for d in domains if d["name"] in nameSet]


class LayerTable:
    _feature: Union[FeatureLayer, Table]

    def __init__(self, feature: Union[FeatureLayer, Table]) -> None:
        self._feature = feature

    def query(
        self,
        outFields: Union[
            str,
            list[str],
        ] = "OBJECTID",
        where: str = "1=1",
        geometry: Union[
            dict[
                Literal["x", "y"],
                float,
            ],
            dict[
                Literal["points"],
                Union[ndarray, list],
            ],
            None,
        ] = None,
        geometryType: Literal[
            "esriGeometryPoint",
            "esriGeometryMultipoint",
            "esriGeometryPolyline",
            "esriGeometryPolygon",
            "esriGeometryEnvelope",
            "esriGeometryMultiPatch",
        ] = "esriGeometryPoint",
        spatialRel: Literal[
            "esriSpatialRelIntersects",
            "esriSpatialRelContains",
            "esriSpatialRelCrosses",
            "esriSpatialRelEnvelopeIntersects",
            "esriSpatialRelIndexIntersects",
            "esriSpatialRelOverlaps",
            "esriSpatialRelTouches",
            "esriSpatialRelWithin",
        ] = "esriSpatialRelIntersects",
        return_geometry=False,
        as_df=True,
    ):

        if geometry is not None:
            if list(geometry.keys()).count("points") != 0:
                if isinstance(geometry["points"], ndarray):
                    geometry["points"] = geometry["points"].tolist()

                geometry["geometry"] = {"points": geometry["points"]}
                del geometry["points"]

            geometry["spatialReference"] = spatialRef  # type: ignore
            geometry["geometryType"] = geometryType  # type: ignore
            geometry["spatialRel"] = spatialRel  # type: ignore

        if isinstance(outFields, list):
            out_fields = ",".join(outFields)  # type: ignore
        else:
            out_fields = outFields

        result = self._feature.query(
            where=where,
            out_fields=out_fields,
            geometry_filter=geometry,  # type: ignore
            return_geometry=return_geometry,
        )
        # workaround for querying Table as_df
        return result.sdf if as_df else result

    def add(self, data: Union[DataFrame, str]):
        return self.__edit_features(data, "add")

    def delete(self, data: Union[DataFrame, str]):
        return self.__edit_features(data, "delete")

    def update(self, data: Union[DataFrame, str]):
        return self.__edit_features(data, "update")

    def __edit_features(
        self,
        data: Union[DataFrame, str],
        operation: Literal["add", "update", "delete"],
    ):
        if isinstance(data, DataFrame):
            featureSet = FeatureSet.from_dataframe(data)
        else:
            featureSet = FeatureSet.from_json(data)

        # hints are wrong. Should be FeatureSet not list[FeatureSet]
        # https://developers.arcgis.com/python/api-reference/arcgis.features.toc.html#arcgis.features.FeatureLayer.edit_features
        if operation == "add":
            result = self._feature.edit_features(adds=featureSet)  # type: ignore
        elif operation == "delete":
            result = self._feature.edit_features(deletes=featureSet)  # type: ignore
        elif operation == "update":
            result = self._feature.edit_features(updates=featureSet)  # type: ignore
        else:
            raise Exception(f"Operation {operation} not supported.")

        if isinstance(data, str):
            data = DF_Util.createFromList(json.loads(data))

        if isinstance(result, dict):
            return concat(
                [
                    data,
                    DataFrame(result[f"{operation}Results"]),
                ],
                axis=1,
            )
        else:
            raise Exception("Unsupported return type.")


class GISFactory:
    _gis: GIS

    def __init__(
        self,
        url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify_cert: bool = True,
    ) -> None:
        self._gis = GIS(url, username, password, verify_cert=verify_cert)

    def FeatureServer(self, url: str) -> Server:
        return Server(
            FeatureLayerCollectionDecorator(
                self._gis._con.token,  # type: ignore
                FeatureLayerCollection(url, self._gis),
            )
        )

    def FeatureLayer(self, url: str) -> LayerTable:
        return LayerTable(FeatureLayer(url, self._gis))

    def FeatureTable(self, url: str) -> LayerTable:
        return LayerTable(Table(url, self._gis))

    def CreateFeature(
        self,
        id_url: Union[str, UUID],
        layer: Optional[int] = None,
    ) -> Union[LayerTable, Server, None]:
        if isinstance(id_url, UUID) and layer is not None:
            return LayerTable(self._gis.content.get(id_url.hex).layers[layer])

        elif isinstance(id_url, str):
            urlParts = urlparse(id_url, "https")

            if urlParts.netloc == "":
                raise Error("Invalid URL")

            path = urlParts.path.split("/")
            title = path[-3 if path[-1].isdigit() else -2]

            collection = self._gis.content.search(
                query=title,
                item_type="Feature Layer Collection",
            )
            filtered = filter(lambda i: i.title == title, collection)
            collection = list(filtered)

            if len(collection) > 1:
                raise Exception("More than one Feature Layer Collection found.")

            if path[-1].isdigit():
                for layer in collection[0].layers:
                    if layer.url[-1] == urlParts.path[-1]:
                        return LayerTable(layer)

                for table in collection[0].tables:
                    if table.url[-1] == urlParts.path[-1]:
                        return LayerTable(table)

            else:
                return Server(
                    FeatureLayerCollectionDecorator(
                        self._gis._con.token,  # type: ignore
                        collection[0],
                    )
                )

        else:
            raise Exception(f"Unsupported id_url: {str(id_url)}, or missing layer")
