import logging
import shapely.geometry
from typing import Generator, Tuple, Optional
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone


logger = logging.getLogger(__name__)


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

    # Calculate adaptive interval based on area size
    area_ha = geometry_collection.area * DEGREES_TO_HECTARES_FACTOR  # Rough conversion to hectares
    adaptive_interval = calculate_adaptive_interval(area_ha, interval)
    
    logger.debug(f"Geometry area: {area_ha:.2f} ha, using interval: {adaptive_interval}")

    for xmin, ymin, xmax, ymax in generate_rectangle_cells(
        envelope.bounds[0],
        envelope.bounds[1],
        envelope.bounds[2],
        envelope.bounds[3],
        interval=adaptive_interval,
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


def calculate_adaptive_interval(area_ha: float, base_interval: float = 1.0) -> float:
    """
    Calculate adaptive interval based on area size to optimize partition count.
    
    Args:
        area_ha: Area in hectares
        base_interval: Base interval in degrees
    
    Returns:
        Optimized interval in degrees
    """
    if area_ha < 1000:  # Small areas (< 1000 ha)
        return base_interval * 2.0  # Larger intervals for small areas
    elif area_ha < 10000:  # Medium areas (1000-10000 ha)
        return base_interval * 1.5
    elif area_ha < 100000:  # Large areas (10000-100000 ha)
        return base_interval
    else:  # Very large areas (> 100000 ha)
        return base_interval * 0.5  # Smaller intervals for very large areas


def optimize_geometry_partitioning(geometry_collection, max_partitions: int = 10) -> Generator:
    """
    Optimize geometry partitioning to limit the number of partitions.
    
    Args:
        geometry_collection: The geometry to partition
        max_partitions: Maximum number of partitions to create
    
    Yields:
        Optimized geometry fragments
    """
    area_ha = geometry_collection.area * 111 * 111
    target_area_per_partition = area_ha / max_partitions
    
    # Calculate optimal interval based on target area
    optimal_interval = (target_area_per_partition / (111 * 111)) ** 0.5
    
    # Ensure interval is within reasonable bounds
    optimal_interval = max(0.1, min(2.0, optimal_interval))
    
    logger.info(f"Optimizing partitioning: area={area_ha:.2f}ha, target_partitions={max_partitions}, interval={optimal_interval:.3f}")
    
    return generate_geometry_fragments(geometry_collection, interval=optimal_interval)
