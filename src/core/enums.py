from enum import Enum


class IfExistsType(str, Enum):
    """Enum for the if_exists parameter of the to_sql method of the DataFrame class."""

    append = "append"
    replace = "replace"
    fail = "fail"
    
    
class SaveGeocodedDataMethodType(str, Enum):
    """Enum for the if_exists parameter of the to_sql method of the DataFrame class."""

    create = "create"
    append = "append"
    extend = "extend"
        
class GeocoderOriginFormatType(str, Enum):
    """Enum for the file format of the geocoder files."""
    csv = "csv"
    sql = "sql"
    
class GeocoderResultSchema(str, Enum):
    """Allowed schemas for the geocoder results."""
    temporal = "temporal"
    
class TableDumpFormat(str, Enum):
    """Allowed schemas for the geocoder results."""
    sql = "sql"
    dump = "dump"
    
class DumpType(str, Enum):
    """Dump types."""
    
    data = "data"
    schema = "schema"
    all = "all"
    
class BuildingClassificationColumnTypes(str, Enum):
    """Columns that can be classified by building preparation"""
    
    residential_status = "residential_status"
    building_levels_residential = "building_levels_residential"
    
class BuildingClassificationTypes(str, Enum):
    """Geometry types for building classification"""
    attribute = "attribute"
    point = "point"
    polygon = "polygon"
    
class MigrationTables(str, Enum):
    """Migration Tables."""
    poi = "poi"
    aoi = "aoi"
    study_area = "study_area"
    sub_study_area = "sub_study_area"
    population = "population"
    building = "building"
    node = "node"
    edge = "edge"

class StudyAreaGeomMigration(str, Enum):
    """Specify whether to choose the buffer or the geometry for the study area."""
    poi = "geom"
    aoi = "buffer_geom_heatmap"
    study_area = "geom"
    sub_study_area = "geom"
    population = "geom"
    building = "geom"
    node = "buffer_geom_heatmap"
    edge = "buffer_geom_heatmap"

class Weekday(str, Enum):
    """Weeksdays."""

    monday = "monday"
    tuesday = "tuesday"
    wednesday = "wednesday"
    thursday = "thursday"
    friday = "friday"
    saturday = "saturday"
    sunday = "sunday"

