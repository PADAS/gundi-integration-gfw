import logging
import shapely.geometry
from typing import Generator, Tuple, Optional
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone


logger = logging.getLogger(__name__)

DEGREES_TO_HECTARES_FACTOR = 111 * 111

class PerformanceMonitor:
    """Monitor performance metrics for data retrieval operations."""
    
    def __init__(self, operation_name: str):
        self.operation_name = operation_name
        self.start_time = None
        self.end_time = None
        self.metrics = {}
        
    def start(self):
        """Start timing the operation."""
        self.start_time = time.time()
        logger.info(f"Starting {self.operation_name}")
        
    def end(self, **metrics):
        """End timing and record additional metrics."""
        self.end_time = time.time()
        self.metrics = metrics
        
        duration = self.end_time - self.start_time
        logger.info(f"Completed {self.operation_name} in {duration:.2f}s - {metrics}")
        
        return duration
    
    def get_summary(self) -> dict:
        """Get performance summary."""
        if not self.start_time or not self.end_time:
            return {}
            
        return {
            "operation": self.operation_name,
            "duration_seconds": self.end_time - self.start_time,
            "metrics": self.metrics
        }


@asynccontextmanager
async def performance_monitor(operation_name: str):
    """Context manager for performance monitoring."""
    monitor = PerformanceMonitor(operation_name)
    monitor.start()
    try:
        yield monitor
    finally:
        monitor.end()


def generate_rectangle_cells(xmin, ymin, xmax, ymax, interval=0.3):
    # Create the grid coordinates for a rectangle.
    while xmin < xmax:
        ycur = ymin
        while ycur < ymax:
            yield (xmin, ycur, min(xmin + interval, xmax), min(ycur + interval, ymax))
            ycur += interval
        xmin += interval


def generate_geometry_fragments(geometry_collection, interval=1.0):
    """Generate geometry fragments with adaptive interval based on area size."""
    
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


