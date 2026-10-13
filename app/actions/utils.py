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


# --- Diagnostics: summarize alerts by confidence level -----------------------

# GFW's nasa_viirs_fire_alerts dataset reports confidence__cat as a single letter.
FIRE_CONFIDENCE_LABELS = {"l": "low", "n": "nominal", "h": "high"}


def fire_alert_confidence(alert) -> str:
    """Expand a NASA VIIRS fire alert's single-letter category to the enum label."""
    raw = str(alert.confidence).lower()
    return FIRE_CONFIDENCE_LABELS.get(raw, raw)


def integrated_alert_confidence(alert) -> str:
    return str(alert.confidence_label).lower()


def _alert_date(alert) -> str:
    recorded = getattr(alert, "recorded_at", None) or getattr(alert, "alert_date", None)
    return recorded.strftime("%Y-%m-%d") if recorded else None


def summarize_by_confidence(alerts, *, confidence_of, confidence_order, lowest_confidence, sample_size=5):
    """
    Group alerts by confidence level and report which would pass the configured threshold.

    :param alerts: parsed alert models (IntegratedAlert or NasaViirsFireAlert)
    :param confidence_of: callable returning the normalized confidence label for an alert
    :param confidence_order: enum members ordered from lowest to highest confidence
    :param lowest_confidence: the configured threshold (enum member)
    :param sample_size: max sample records kept per level
    """
    levels = [level.value for level in confidence_order]
    threshold_index = levels.index(lowest_confidence.value)
    sent_levels = levels[threshold_index:]

    available = {level: 0 for level in levels}
    samples = {level: [] for level in levels}

    for alert in alerts:
        label = confidence_of(alert)
        available[label] = available.get(label, 0) + 1
        bucket = samples.setdefault(label, [])
        if len(bucket) < sample_size:
            bucket.append({
                "latitude": alert.latitude,
                "longitude": alert.longitude,
                "date": _alert_date(alert),
                "confidence": label,
            })

    sent = {level: available[level] for level in sent_levels}

    return {
        "lowest_confidence": lowest_confidence.value,
        "available_total": sum(available.values()),
        "sent_total": sum(sent.values()),
        "available_by_confidence": available,
        "sent_by_confidence": sent,
        "samples": samples,
    }


def format_sample_report(result: dict) -> str:
    """Render a sample_data result as plain text for the CLI."""
    if "error" in result and "fire_alerts" not in result:
        return f"ERROR: {result['error']}"

    lines = [
        f"AOI: {result.get('aoi_name')} ({result.get('aoi_id')})  geostore: {result.get('geostore_id')}",
        f"Sampled at: {result.get('sampled_at')}",
    ]

    for label, key in (("Fire alerts", "fire_alerts"), ("Integrated alerts", "integrated_alerts")):
        feed = result.get(key) or {}
        lines.append("")
        if not feed.get("enabled", True):
            lines.append(f"{label}: disabled")
            continue
        header = f"{label} ({feed.get('dataset')}) - last {feed.get('lookback_days')} days"
        if "error" in feed:
            lines.append(f"{header}: query failed: {feed['error']}")
        else:
            lines.append(f"{header}, threshold '{feed.get('lowest_confidence')}'")
            lines.append(f"  {'confidence':<12}{'available':>10}{'sent':>8}")
            sent = feed.get("sent_by_confidence", {})
            for level, count in feed.get("available_by_confidence", {}).items():
                sent_count = str(sent[level]) if level in sent else "-"
                lines.append(f"  {level:<12}{count:>10}{sent_count:>8}")
            lines.append(f"  {'total':<12}{feed.get('available_total', 0):>10}{feed.get('sent_total', 0):>8}")

        state_bits = []
        if "dataset_updated_on" in feed:
            state_bits.append(f"dataset updated: {(feed.get('dataset_updated_on') or 'unknown')[:10]}")
        if "last_processed_dataset_update" in feed:
            state_bits.append(f"last processed: {(feed.get('last_processed_dataset_update') or 'never')[:10]}")
        if "quiet_period_active" in feed:
            state_bits.append(f"quiet period: {'active' if feed['quiet_period_active'] else 'no'}")
        if "pending_jobs" in feed:
            state_bits.append(f"pending jobs: {feed['pending_jobs']}")
        if state_bits:
            lines.append("  " + "  |  ".join(state_bits))
        for err in feed.get("state_errors", []):
            lines.append(f"  warning: {err}")

        samples = [s for bucket in feed.get("samples", {}).values() for s in bucket]
        if samples:
            lines.append("  samples:")
            for s in samples:
                lines.append(f"    {s['date']}  {s['latitude']:.5f}, {s['longitude']:.5f}  {s['confidence']}")

    lines.append("")
    lines.append(result.get("message", ""))
    return "\n".join(lines)
