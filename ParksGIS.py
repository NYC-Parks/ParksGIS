from arcgis.features import (
    FeatureLayer,
    FeatureSet,
    Table,
    FeatureLayerCollection,
)
from arcgis.geometry import SpatialReference
from arcgis.gis import GIS, Item
from json import dumps, loads
from numpy import ndarray
from pandas import DataFrame, Series, concat, json_normalize
from requests import Response, post
from typing import Any, Literal, Optional
from urllib import parse
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
    adds: DataFrame | Series
    updates: DataFrame | Series

    def __init__(
        self,
        id: int,
        adds: DataFrame | Series | list[str] | None = None,
        updates: DataFrame | Series | list[str] | None = None,
    ):
        self.id = id
        if adds is not None:
            self.adds = json_normalize(adds) if isinstance(adds, list) else adds
        if updates is not None:
            self.updates = (
                json_normalize(updates) if isinstance(updates, list) else updates
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
    ) -> None:
        self.layerId = id
        self.outFields = ",".join(fields)
        self.where = where


class LayerDomainNames:
    id: int
    names: list[str]

    def __init__(
        self,
        id: int,
        names: list[str] = ["*"],
    ) -> None:
        self.id = id
        self.names = names


class LayerServerGen:
    id: int
    serverGen: int  # EPOCH

    def __init__(
        self,
        id: int,
        serverGen: int,
    ) -> None:
        self.id = id
        self.serverGen = serverGen


class Server:
    _token: str
    _verify_cert: bool
    _featureLayerCollection: FeatureLayerCollection

    def __init__(
        self,
        token: str,
        verify_cert: bool,
        collection_or_item: FeatureLayerCollection | Item,
    ) -> None:
        self._token = token
        self._verify_cert = verify_cert
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
                        )
                    ).append(item.features)
            for table in self._featureLayerCollection.properties.tables:
                if table in self._featureLayerCollection.properties.tables:
                    if table.id == item.id:
                        LayerTable(
                            Table(
                                self._featureLayerCollection.url + "/" + str(item.id),
                                self._featureLayerCollection._gis,
                            )
                        ).append(item.features)

    # apply_edits is missing from API
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
                dict["adds"] = [
                    {"attributes": item._asdict()}
                    for item in dict["adds"].itertuples(index=False)
                ]
            if "updates" in dict:
                dict["updates"] = [
                    {"attributes": item._asdict()}
                    for item in dict["updates"].itertuples(index=False)
                ]
            edits.append(dict)
        # print(edits)

        response: Response = post(
            self._featureLayerCollection.url + "/applyEdits",
            {
                "edits": dumps(edits),
                "gdbVersion": gdbVersion,
                "rollbackOnFailure": rollbackOnFailure,
                "useGlobalIds": useGlobalIds,
                "returnEditMoment": returnEditMoment,
                "returnServiceEditsOption": returnServiceEditsOption,
                "f": "json",
                "token": self._token,
            },
            verify=self._verify_cert,
        )
        # print(response)

        return list(self.__response_handler(response))

    def extract_changes(
        self,
        layer_server_gen: list[LayerServerGen],
        layer_query: list[LayerQuery] | None = None,
        inserts: bool = True,
        updates: bool = True,
        deletes: bool = False,
        return_ids_only: bool = True,
    ):
        changes = self._featureLayerCollection.extract_changes(
            layers=[layer.id for layer in layer_server_gen],
            layer_servergen=[layer.__dict__ for layer in layer_server_gen],
            queries=(
                None
                if layer_query is None
                else {
                    str(layer.layerId): {"where": layer.where}
                    for _, layer in enumerate(layer_query)
                }
            ),
            return_inserts=inserts,
            return_updates=updates,
            return_deletes=deletes,
            return_ids_only=True,
        )

        if return_ids_only:
            return changes

        raise Exception("Not Implemented")

    # There is a bug in argcis.features.FeatureLayerCollection.query()
    # LayerDefinitionFilter is not being added to query param
    def query(
        self,
        layerDefinitions: list[LayerQuery],
        geometry: (
            dict[
                Literal["x", "y"],
                float,
            ]
            | dict[
                Literal["points"],
                ndarray | list,
            ]
            | None
        ) = None,
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
    ) -> dict[int, DataFrame]:
        layerDefs = [layer.__dict__ for layer in layerDefinitions]
        # print(layerDefs)

        if as_df:
            self._featureLayerCollection._populate_layers()
            result = {}

            features = {
                feature.properties["id"]: feature
                for _, feature in enumerate(
                    self._featureLayerCollection.layers
                    + self._featureLayerCollection.tables
                )
            }

            for layer in layerDefinitions:
                feature = features[layer.layerId]
                result[layer.layerId] = LayerTable(feature).query(
                    where=layer.where,
                    outFields=layer.outFields,
                )

            return result

        else:
            response: Response = post(
                self._featureLayerCollection.url + "/query",
                {
                    "layerDefs": dumps(layerDefs),
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
                verify=self._verify_cert,
            )

            return self.__response_handler(response)

    def query_domains(
        self,
        layer_domain_names: list[LayerDomainNames],
    ) -> list[bytes]:
        layerDomains = self._featureLayerCollection.query_domains(
            layers=[layer.id for layer in layer_domain_names]
        )

        if layer_domain_names[0].names[0] == "*":
            return list(layerDomains)

        domainNameSet = {name for layer in layer_domain_names for name in layer.names}
        return [domain for domain in layerDomains if domain["name"] in domainNameSet]

    def __response_handler(self, response: Response) -> dict | list:
        result = response.json()

        if not response.ok or (isinstance(result, dict) and "error" in result):
            raise Exception(result)

        return result


class LayerTable:
    _feature: FeatureLayer | Table

    def __init__(
        self,
        feature: FeatureLayer | Table,
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
        outFields: str | list[str] = "OBJECTID",
        where: str = "1=1",
        geometry: (
            dict[
                Literal["x", "y"],
                float,
            ]
            | dict[
                Literal["points"],
                ndarray | list,
            ]
            | None
        ) = None,
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

    def add(self, data: DataFrame | str):
        return self.__edit_features(data, "add")

    def delete(self, data: DataFrame | str):
        return self.__edit_features(data, "delete")

    def update(self, data: DataFrame | str):
        return self.__edit_features(data, "update")

    def __edit_features(
        self,
        data: DataFrame | str,
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
            data = DF_Util.createFromList(loads(data))

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
    _verify_cert: bool

    def __init__(
        self,
        url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify_cert: bool = True,
    ) -> None:
        self._verify_cert = verify_cert
        self._gis = GIS(url, username, password, verify_cert=verify_cert)

    def feature_server(self, url: str) -> Server:
        return Server(
            self._gis._con.token,  # type: ignore
            FeatureLayerCollection(url, self._gis),
        )

    def feature_layer(self, url: str) -> LayerTable:
        return LayerTable(FeatureLayer(url, self._gis))

    def feature_table(self, url: str) -> LayerTable:
        return LayerTable(Table(url, self._gis))

    def create_feature(
        self,
        id_url: str | UUID,
        layer: Optional[int] = None,
    ) -> LayerTable | Server | Any:
        if isinstance(id_url, UUID) and layer is not None:
            return LayerTable(self._gis.content.get(id_url.hex).layers[layer])

        elif isinstance(id_url, str):
            urlParts = parse.urlparse(id_url, "https")
            type = "Feature Layer Collection"

            if urlParts.netloc == "":
                raise Exception("Invalid URL")

            path = urlParts.path.split("/")
            title = path[-3 if path[-1].isdigit() else -2]

            collections = self._gis.content.search(
                query=f'title:"{title}"',
                item_type=type,
            )
            filtered_collections = list(
                filter(lambda layer: layer.title == title, collections)
            )

            length = len(filtered_collections)
            if length == 0 | 1 < length:
                raise Exception(f"{type} not found.")

            if path[-1].isdigit():
                for layer in filtered_collections[0].layers:
                    if layer.url[-1] == urlParts.path[-1]:
                        return LayerTable(layer)

                for table in filtered_collections[0].tables:
                    if table.url[-1] == urlParts.path[-1]:
                        return LayerTable(table)

            else:
                return Server(
                    self._gis._con.token,  # type: ignore
                    self._verify_cert,
                    filtered_collections[0],
                )

        else:
            raise Exception(f"Unsupported id_url: {str(id_url)}, or missing layer")
