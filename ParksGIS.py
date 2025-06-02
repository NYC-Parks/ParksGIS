from arcgis.features import (
    FeatureLayer,
    FeatureSet,
    Table,
    FeatureLayerCollection,
)
from arcgis.geometry import SpatialReference
from arcgis.gis import GIS, Item
from itertools import chain
from json import dumps
from logging import Logger, getLogger
from numpy import ndarray
from pandas import DataFrame, Series, concat, json_normalize
from requests import Response, post
from typing import Any, Literal, Optional
from urllib import parse
from uuid import UUID

spatialRef = SpatialReference({"wkid": 102718, "latestWkid": 2263})


def log_sql(
    logger: Logger,
    layer: int,
    fields: list[str],
    where: str,
    object_ids: Optional[str] = None,
) -> None:
    template = f"\n\tSelect {fields}\n\tFrom Layer-{layer}\n\tWhere {where}"
    if object_ids:
        template = template + f"\n\tObject IDs: {object_ids}"
    logger.debug(template)


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
    where: str
    object_ids: Optional[str] = None
    outFields: str

    def __init__(
        self,
        id: int,
        fields: list[str] = ["OBJECTID"],
        where: str = "1=1",
        object_ids: Optional[str] = None,
    ) -> None:
        self.layerId = id
        self.where = where
        self.object_ids = object_ids
        self.outFields = ",".join(fields)


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
    _logger: Logger
    _verify_cert: bool
    _featureLayerCollection: FeatureLayerCollection

    def __init__(
        self,
        token: str,
        verify_cert: bool,
        collection_or_item: FeatureLayerCollection | Item,
        logger: Logger,
    ) -> None:
        self._token = token
        self._logger = logger
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
                    Feature(
                        FeatureLayer(
                            self._featureLayerCollection.url + "/" + str(item.id),
                            self._featureLayerCollection._gis,
                        ),
                        self._logger,
                    ).append(item.features)
            for table in self._featureLayerCollection.properties.tables:
                if table in self._featureLayerCollection.properties.tables:
                    if table.id == item.id:
                        Feature(
                            Table(
                                self._featureLayerCollection.url + "/" + str(item.id),
                                self._featureLayerCollection._gis,
                            ),
                            self._logger,
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
        response: Response = post(
            self._featureLayerCollection.url + "/applyEdits",
            {
                "edits": dumps(self._to_attributes(layer_edits)),
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

        return list(self.__response_handler(response))

    def _to_attributes(self, layer_edits: list[LayerEdits]) -> list[dict[str, Any]]:
        edits = []

        for edit in layer_edits:
            dict = edit.__dict__
            for key in dict.keys():
                if key in ["adds", "updates"]:
                    dict[key] = [
                        {"attributes": row._asdict()} for row in dict[key].itertuples()
                    ]
            edits.append(dict)

        self._logger.debug(edits)
        return edits

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
        else:
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
        for layer in layerDefinitions:
            log_sql(
                self._logger,
                layer.layerId,
                layer.outFields.split(","),
                layer.where,
                layer.object_ids,
            )

        if as_df:
            self._featureLayerCollection._populate_layers()
            result = {}

            features = {
                feature.properties["id"]: feature
                for feature in chain(
                    self._featureLayerCollection.layers,
                    self._featureLayerCollection.tables,
                )
            }

            for layer in layerDefinitions:
                feature = features[layer.layerId]
                result[layer.layerId] = Feature(
                    feature,
                    self._logger,
                ).query(
                    where=layer.where,
                    object_ids=layer.object_ids,
                    out_fields=layer.outFields,
                )

            return result

        else:
            response: Response = post(
                self._featureLayerCollection.url + "/query",
                {
                    "layerDefs": dumps([layer.__dict__ for layer in layerDefinitions]),
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


class Feature:
    _logger: Logger
    _feature: FeatureLayer | Table

    def __init__(
        self,
        feature: FeatureLayer | Table,
        logger: Logger,
    ) -> None:
        self._logger = logger
        self._feature = feature

    def append(
        self,
        featutes: list,
    ):
        exists = {
            self.query(
                object_ids=",".join(feature["ObjectID"] for feature in featutes)
                # as_df=False,
            )["OBJECTID"]
        }

        adds = [feature for feature in featutes if feature["OBJECTID"] not in exists]
        updates = [feature for feature in featutes if feature["OBJECTID"] in exists]

        return self._feature.edit_features(adds=adds, updates=updates)

    def query(
        self,
        out_fields: str | list[str] = "OBJECTID",
        where: str = "1=1",
        object_ids: Optional[str] = None,
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
        log_sql(
            self._logger,
            self._feature.properties["id"],
            out_fields if isinstance(out_fields, list) else out_fields.split(","),
            where,
            object_ids,
        )

        if geometry is not None:
            if list(geometry.keys()).count("points") != 0:
                if isinstance(geometry["points"], ndarray):
                    geometry["points"] = geometry["points"].tolist()

                geometry["geometry"] = {"points": geometry["points"]}
                del geometry["points"]

            geometry["spatialReference"] = spatialRef  # type: ignore
            geometry["geometryType"] = geometryType  # type: ignore
            geometry["spatialRel"] = spatialRel  # type: ignore

        if isinstance(out_fields, list):
            out_fields = ",".join(out_fields)  # type: ignore
        else:
            out_fields = out_fields

        result = self._feature.query(
            where=where,
            object_ids=object_ids,  # type: ignore *type definition and implementation mismatch*
            out_fields=out_fields,
            geometry_filter=geometry,  # type: ignore
            return_geometry=return_geometry,
        )
        # workaround for querying Table as_df
        return result.sdf if as_df else result

    def add(self, data: DataFrame):
        return self.__edit_features(data, "add")

    def delete(self, data: DataFrame):
        return self.__edit_features(data, "delete")

    def update(self, data: DataFrame):
        return self.__edit_features(data, "update")

    def __edit_features(
        self,
        data: DataFrame,
        operation: Literal["add", "update", "delete"],
    ):
        # if isinstance(data, DataFrame):
        featureSet = FeatureSet.from_dataframe(data)
        # else:
        #     featureSet = FeatureSet.from_json(data)

        # hints are wrong. Should be FeatureSet not list[FeatureSet]
        # https://developers.arcgis.com/python/api-reference/arcgis.features.toc.html#arcgis.features.FeatureLayer.edit_features
        match operation:
            case "add":
                result = self._feature.edit_features(adds=featureSet)  # type: ignore
            case "delete":
                result = self._feature.edit_features(deletes=featureSet)  # type: ignore
            case "update":
                result = self._feature.edit_features(updates=featureSet)  # type: ignore
            case _:
                raise Exception(f"Operation {operation} not supported.")

        # if isinstance(data, str):
        #     data = DF_Util.createFromList(loads(data))

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
    _services_base_url: str
    _logger = getLogger("[ parks_gis ]")

    def __init__(
        self,
        url: Optional[str] = None,
        username: Optional[str] = None,
        password: Optional[str] = None,
        verify_cert: bool = True,
    ) -> None:
        self._verify_cert = verify_cert
        self._gis = GIS(url, username, password, verify_cert=verify_cert)
        self._services_base_url = (
            self._get_base_url(self._gis.url if url is None else url)
            + "/server/rest/services"
        )

    def _get_base_url(self, url: str) -> str:
        parsed_url = parse.urlparse(url, "https")
        if parsed_url.netloc == "":
            raise Exception("Invalid URL")

        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    def create(
        self,
        id_path: str | UUID,
        layer: Optional[int] = None,
    ) -> Feature | Server | Any:
        if isinstance(id_path, UUID) and layer is not None:
            return Feature(
                self._gis.content.get(id_path.hex).layers[layer],
                self._logger,
            )

        elif isinstance(id_path, str):
            path_parts = id_path.split("/")
            collection = self._get_feature_layer_collection(title=path_parts[1])

            if path_parts[-1].isdigit():
                feature = self._get_feature(path_parts[2], collection)
                return Feature(feature, self._logger)
            else:
                return Server(
                    self._gis._con.token,  # type: ignore
                    self._verify_cert,
                    collection,
                    self._logger,
                )

        else:
            raise Exception(f"Unsupported id_url: {str(id_path)}, or missing layer")

    def _get_feature_layer_collection(self, title: str) -> FeatureLayerCollection:
        search_result = self._gis.content.search(
            query=f'title:"{title}"',
            item_type="Feature Layer Collection",
        )

        collections = list(filter(lambda layer: layer.title == title, search_result))

        if len(collections) == 0 or 1 < len(collections):
            raise Exception(f"{title} not found.")

        return collections[0]

    def _get_feature(self, feature: str, collection: FeatureLayerCollection) -> Any:
        for layer in collection.layers:
            if layer.url[-1] == feature:
                return layer

        for table in collection.tables:
            if table.url[-1] == feature:
                return table

    def feature_server(self, path: str) -> Server:
        return Server(
            self._gis._con.token,  # type: ignore
            FeatureLayerCollection(self._services_base_url + path, self._gis),
            self._logger,
        )

    def feature_layer(self, path: str) -> Feature:
        return Feature(
            FeatureLayer(self._services_base_url + path, self._gis),
            self._logger,
        )

    def feature_table(self, path: str) -> Feature:
        return Feature(
            Table(self._services_base_url + path, self._gis),
            self._logger,
        )
