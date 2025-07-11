import logging
import shapely.geometry


logger = logging.getLogger(__name__)


def generate_rectangle_cells(xmin, ymin, xmax, ymax, interval=0.3):
    # Create the grid coordinates for a rectangle.
    while xmin < xmax:
        ycur = ymin
        while ycur < ymax:
            yield (xmin, ycur, min(xmin + interval, xmax), min(ycur + interval, ymax))
            ycur += interval
        xmin += interval


def generate_geometry_fragments(geometry_collection, interval=1.0):

    geometry_collection = geometry_collection.simplify(tolerance=0.0005, preserve_topology=True)

    # It is possible for the simplify function will produce an invalid result.
    # Buffer it to produce a valid geometry.
    if not geometry_collection.is_valid:
        geometry_collection = geometry_collection.buffer(0)

    envelope = geometry_collection.envelope

    # Check if envelope has valid bounds
    if not envelope.bounds:
        logger.error(f"Geometry collection has no bounds: {geometry_collection}")
        raise ValueError("The geometry collection does not have valid envelope bounds.")


    for xmin, ymin, xmax, ymax in generate_rectangle_cells(
        envelope.bounds[0],
        envelope.bounds[1],
        envelope.bounds[2],
        envelope.bounds[3],
        interval=interval,
    ):
        rectangle_shape = shapely.geometry.Polygon(
            [(xmin, ymin), (xmin, ymax), (xmax, ymax), (xmax, ymin)]
        )

        intersection = rectangle_shape.intersection(geometry_collection)
        if not intersection.is_empty:
            if intersection.geom_type == 'Polygon':
                yield intersection
            elif intersection.geom_type in ['MultiPolygon', 'GeometryCollection']:
                for geom in intersection.geoms:
                    if geom.geom_type == 'Polygon' and not geom.is_empty:
                        yield geom
