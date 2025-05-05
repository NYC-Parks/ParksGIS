# ParksGIS Module

## Overview

The `ParksGIS` module is an abstraction over the ArcGIS Python library. It simplifies interactions with the ArcGIS REST API by providing a higher-level interface for querying, editing, and managing GIS data layers and tables.

## Features

- Query GIS layers and tables with custom filters and geometry.
- Append, update, and delete features in GIS layers and tables.
- Extract changes from layers using server generation numbers.
- Simplified creation of GIS objects like feature layers, tables, and servers.

## Usage

### Initialization

To use the `GISFactory` class, initialize it with the ArcGIS AGOL/Enterprise portal URL and credentials:

```python
from ParksGIS import GISFactory

gis_factory = GISFactory(
    url="https://arcgis.com",
    username="your-username",
    password="your-password"
)
```

### Working with Feature Layers and Tables

#### Create a Feature Layer
```python
feature_layer = gis_factory.feature_layer("https://example.arcgis.com/FeatureServer/0")
```

#### Query a Feature Layer
```python
result = feature_layer.query(
    where="POPULATION > 1000",
    outFields=["OBJECTID", "NAME", "POPULATION"],
    as_df=True
)
print(result)
```

#### Append Features to a Layer
```python
features = [{"OBJECTID": 1, "NAME": "Park A", "POPULATION": 5000}]
feature_layer.append(features)
```

### Managing Servers

#### Create a Feature Server
```python
server = gis_factory.feature_server("https://example.arcgis.com/FeatureServer")
```

#### Extract Changes from a Server
```python
from ParksGIS import LayerServerGen

layer_server_gen = [LayerServerGen(id=0, serverGen=1234567890)]
changes = server.extract_changes(layer_server_gen)
print(changes)
```

## Logging

The module uses Python's built-in logging library. To enable logging, configure the logger in your application:

```python
import logging

logging.basicConfig(level=logging.DEBUG)
```

## Error Handling

The module raises exceptions for API errors and invalid operations. Ensure to wrap API calls in try-except blocks to handle errors gracefully.

```python
try:
    result = feature_layer.query(where="INVALID QUERY")
except Exception as e:
    print(f"Error: {e}")
```

## License

This module is licensed under the MIT License. See the LICENSE file for details.