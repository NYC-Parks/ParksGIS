from arcgis.features import (
    FeatureLayer,
    FeatureSet,
    Table,
    FeatureLayerCollection,
)
from arcgis.geometry import SpatialReference
from arcgis.gis import GIS, Item
import json
from numpy import ndarray, number
from pandas import DataFrame, concat, json_normalize, to_datetime
import requests
from typing import Literal, Optional, Union
from urllib.parse import urlparse
from uuid import UUID

spatialRef = SpatialReference({"wkid": 102718, "latestWkid": 2263})


class LayerAppend:
    id: int
    features: list

    def __init__(self, id: int, features: list):
        self.id = id
        self.features = features


class LayerEdits:
    id: int
    adds: DataFrame
    updates: DataFrame

    def __init__(
        self,
        id: int,
        adds: Optional[Union[DataFrame, list[str]]] = None,
        updates: Optional[Union[DataFrame, list[str]]] = None,
    ):
        self.id = id
        if adds is not None:
            self.adds = adds if isinstance(adds, DataFrame) else json_normalize(adds)
        if updates is not None:
            self.updates = (
                updates if isinstance(updates, DataFrame) else json_normalize(updates)
            )


class LayerQuery:
    layerId: int
    outFields: str
    where: str

    def __init__(
        self,
        id: int,
        fields: list[str] = ["OBJECTID"],
        where: str = "1=1",
    ):
        self.layerId = id
        self.outFields = ",".join(fields)
        self.where = where


class LayerDomainNames:
    id: int
    names: list[str]

    def __init__(
        self,
        id: int,
        names: list[str],
    ) -> None:
        self.id = id
        self.names = names


class LayerServerGen:
    id: int
    serverGen: int  # EPOCH time to start from in milliseconds

    def __init__(
        self,
        id: int,
        serverGen: int,
    ) -> None:
        self.id = id
        self.serverGen = serverGen * 1000


class Server:
    _featureLayerCollection: FeatureLayerCollection
    _token: str

    def __init__(
        self,
        token: str,
        collection_or_item: Union[FeatureLayerCollection, Item],
    ) -> None:
        self._token = token
        if isinstance(collection_or_item, FeatureLayerCollection):
            self._featureLayerCollection = collection_or_item
        else:
            self._featureLayerCollection = FeatureLayerCollection.fromitem(
                collection_or_item
            )

    def append(self, layer_appends: list[LayerAppend]):
        for item in layer_appends:
            for layer in self._featureLayerCollection.properties.layers:
                if layer.id == item.id:
                    LayerTable(
                        FeatureLayer(
                            self._featureLayerCollection.url + "/" + str(item.id),
                            self._featureLayerCollection._gis,
                            self,
                        )
                    ).append(item.features)
            for table in self._featureLayerCollection.properties.tables:
                if table in self._featureLayerCollection.properties.tables:
                    if table.id == item.id:
                        LayerTable(
                            Table(
                                self._featureLayerCollection.url + "/" + str(item.id),
                                self._featureLayerCollection._gis,
                                self,
                            )
                        ).append(item.features)

    def apply_edits(
        self,
        layer_edits: list[LayerEdits],
        gdbVersion: Optional[str] = None,
        rollbackOnFailure: bool = True,
        useGlobalIds: bool = False,
        returnEditMoment: bool = False,
        returnServiceEditsOption: Literal[
            "none",
            "originalAndCurrentFeatures",
        ] = "none",
    ) -> list[str]:
        edits = []
        for edit in layer_edits:
            dict = edit.__dict__
            if "adds" in dict:
                dict["adds"] = {
                    "attributes": dict["adds"].to_json(orient="records"),
                }
            if "updates" in dict:
                dict["updates"] = {
                    "attributes": dict["updates"].to_json(orient="records"),
                }
            edits.append(dict)

        response = requests.post(
            self._featureLayerCollection.url + "/applyEdits",
            {
                "edits": json.dumps(edits),
                "gdbVersion": gdbVersion,
                "rollbackOnFailure": rollbackOnFailure,
                "useGlobalIds": useGlobalIds,
                "returnEditMoment": returnEditMoment,
                "returnServiceEditsOption": returnServiceEditsOption,
                "f": "json",
                "token": self._token,
            },
        ).json()

        if not isinstance(response, list) and response.get("error") is not None:
            raise Exception(response["error"])

        return response

    def extractChanges(
        self,
        layer_servergen: list[LayerServerGen],
        inserts=True,
        updates=True,
        deletes=False,
    ):
        return self._featureLayerCollection.extract_changes(
            layers=[i.id for i in layer_servergen],
            layer_servergen=[i.__dict__ for i in layer_servergen],
            return_inserts=inserts,
            return_updates=updates,
            return_deletes=deletes,
            return_ids_only=True,
        )

    # There is a bug in argcis.features.FeatureLayerCollection.query()
    # layer_defs_filter is not being added to query param
    def query(
        self,
        layerDefinitions: list[LayerQuery],
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
            "esriGeometryEnvelope",
            "esriGeometryPoint",
            "esriGeometryPolyline",
            "esriGeometryPolygon",
            "esriGeometryMultipoint",
        ] = "esriGeometryEnvelope",
        spatialRelationship: Literal[
            "esriSpatialRelIntersects",
            "esriSpatialRelContains",
            "esriSpatialRelCrosses",
            "esriSpatialRelEnvelopeIntersects",
            "esriSpatialRelIndexIntersects",
            "esriSpatialRelOverlaps",
            "esriSpatialRelTouches",
            "esriSpatialRelWithin",
        ] = "esriSpatialRelIntersects",
        returnDistinctValues: bool = False,
        returnGeometry: bool = False,
        returnCountOnly: bool = False,
        returnZ: bool = False,
        returnM: bool = False,
        as_df: bool = True,
    ) -> Union[dict[number, DataFrame], str]:
        layerDefs = [layer.__dict__ for layer in layerDefinitions]
        # print(layerDefs)

        response = requests.post(
            self._featureLayerCollection.url + "/query",
            {
                "layerDefs": json.dumps(layerDefs),
                "geometry": geometry,
                "geometryType": geometryType,
                "spatialRel": spatialRelationship,
                "returnDistinctValues": returnDistinctValues,
                "returnGeometry": returnGeometry,
                "returnCountOnly": returnCountOnly,
                "returnZ": returnZ,
                "returnM": returnM,
                "multipatchOption": "xyFootprint",
                "returnTrueCurves": False,
                "sqlFormat": "none",
                "f": "json",
                "token": self._token,
            },
        ).json()

        if response.get("error") != None:
            raise Exception(response["error"])

        if not as_df:
            return response

        # if geometry is not None and 0 < geometry.keys().count("points"):
        #     df["Point"] = geometry["points"]
        #         if isinstance(geometry["points"], ndarray):
        #             geometry["points"] = geometry["points"].tolist()

        #         geometry["geometry"] = {"points": geometry["points"]}
        #         del geometry["points"]

        #     geometry["spatialReference"] = spatialRef  # type: ignore
        #     geometry["geometryType"] = geometryType  # type: ignore
        #     geometry["spatialRel"] = spatialRel  # type: ignore

        dict = {}

        for layer in response["layers"]:
            columns = [field["name"] for field in response["layers"][0]["fields"]]
            if returnGeometry:
                columns.append("Geometry")

            df = DataFrame(columns=columns)

            for feature in layer["features"]:
                if returnGeometry:
                    feature["Geometry"] = feature.geometry
                df.loc[len(df)] = feature["attributes"]

            dict[layer["id"]] = df

        return dict

        # if fields is not None:
        #     for field in fields:
        #         if len(features) == 1:
        #             df[field["name"]] = next(
        #                 iter(features[0]["attributes"].values())
        #             )

        #         else:
        #             df[field["name"]] = None

        #             for i in df.index:
        #                 for feature in features:
        #                     geo = feature["geometry"]
        #                     geo["spatialReference"] = spatialRef
        #                     ploygon = Geometry(geometry)

        #                     xy = df.at[i, "Point"]
        #                     point = Geometry(
        #                         {
        #                             "x": xy[0],
        #                             "y": xy[1],
        #                             "spatialReference": spatialRef,
        #                         }
        #                     )

        #                     if ploygon.intersect(point):
        #                         colName = field["name"]
        #                         value = feature["attributes"][colName]
        #                         df.at[i, colName] = value

    def queryDomains(
        self,
        layer_domainnames: list[LayerDomainNames],
    ):
        domains = self._featureLayerCollection.query_domains(
            [l.id for l in layer_domainnames]
        )
        nameSet = {item for l in layer_domainnames for item in l.names}
        return [d for d in domains if d["name"] in nameSet]


class LayerTable:
    _feature: Union[FeatureLayer, Table]

    def __init__(
        self,
        feature: Union[FeatureLayer, Table],
    ) -> None:
        self._feature = feature

    def append(
        self,
        featutes: list,
    ):
        exists = {
            self.query(
                where="OBJECTID IN ("
                + ",".join(feature["ObjectID"] for feature in featutes)
                + ")",
                # as_df=False,
            )["OBJECTID"]
        }

        adds = [feature for feature in featutes if feature["OBJECTID"] not in exists]
        updates = [feature for feature in featutes if feature["OBJECTID"] in exists]

        return self._feature.edit_features(adds=adds, updates=updates)

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
            self._gis._con.token,  # type: ignore
            FeatureLayerCollection(url, self._gis),
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
                raise Exception("Invalid URL")

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
                    self._gis._con.token,  # type: ignore
                    collection[0],
                )

        else:
            raise Exception(f"Unsupported id_url: {str(id_url)}, or missing layer")
