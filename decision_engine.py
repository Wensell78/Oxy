"""
decision_engine.py

Role:
 - Rule-based Decision Engine for interpreting air-quality observations
   (AQI and PM2.5) and producing an actionable, UI-friendly output.

Design principles (for Methods):
 - Transparent, deterministic rules (no ML). Levels map to common AQI/PM2.5
   breakpoints used in public health guidance. When either indicator is
   elevated, the overall risk reflects the worse pollutant.
 - Recommendations are short, specific, and health-oriented.

Returned structure (example):
 {
   "risk_level": "Unhealthy",        # one of: Normal/Moderate/Unhealthy/Hazardous
   "score": 2,                        # ordinal 0..3 (0=Normal)
   "reasons": ["AQI=120 (Unhealthy)", "PM2.5=40 µg/m3 (Unhealthy)"],
   "recommendations": ["Avoid vigorous outdoor exercise", ...],
   "explanation": "AQI and PM2.5 are both in the Unhealthy range, therefore..."
 }

The module performs only interpretation; it does not fetch or store data.
"""

from typing import Dict, List, Any, Optional


LEVEL_NAMES = ["Normal", "Moderate", "Unhealthy", "Hazardous"]


def _aqi_to_level(aqi: Optional[float]) -> int:
    """Map AQI value to ordinal level 0..3.

    Breakpoints (simplified, health-oriented):
    0-50 -> Normal
    51-100 -> Moderate
    101-200 -> Unhealthy
    201+ -> Hazardous
    """
    if aqi is None:
        return 0
    try:
        a = float(aqi)
    except Exception:
        return 0

    # Support two AQI formats:
    # - OpenWeather category 1..5 (small integers). Treat any aqi <= 5
    #   as a category value and map to risk levels.
    # - Classical numeric AQI index (0..500+). Values > 5 are treated as
    #   numeric AQI and mapped using standard breakpoints.
    if a <= 5:
        # Map OpenWeather category -> risk level.
        # Proposal: map 1->Normal, 2->Moderate, 3->Unhealthy, 4->Unhealthy, 5->Hazardous.
        # Rationale: OpenWeather categories 4 are described as "poor" which
        # typically corresponds to unhealthy conditions but not always the
        # most extreme; 5 represents very poor/very high pollution and is
        # best treated as Hazardous.
        cat = int(round(a))
        if cat <= 1:
            return 0
        if cat == 2:
            return 1
        if cat == 3:
            return 2
        if cat == 4:
            return 2
        return 3

    # Numeric AQI (standard index)
    if a <= 50:
        return 0
    if a <= 100:
        return 1
    if a <= 200:
        return 2
    return 3


def _pm25_to_level(pm25: Optional[float]) -> int:
    """Map PM2.5 (µg/m3) to ordinal level 0..3.

    Breakpoints (approximate and conservative):
    0-12 -> Normal
    12.1-35.4 -> Moderate
    35.5-55.4 -> Unhealthy
    55.5+ -> Hazardous
    """
    if pm25 is None:
        return 0
    try:
        p = float(pm25)
    except Exception:
        return 0
    if p <= 12.0:
        return 0
    if p <= 35.4:
        return 1
    if p <= 55.4:
        return 2
    return 3


def _reason_strings(aqi: Optional[float], pm25: Optional[float]) -> List[str]:
    parts: List[str] = []
    if aqi is not None:
        # Distinguish between category (1..5) and numeric AQI index
        try:
            a = float(aqi)
        except Exception:
            parts.append("AQI=invalid")
        else:
            if a <= 5:
                parts.append(f"AQI(category)={int(round(a))} ({LEVEL_NAMES[_aqi_to_level(a)]})")
            else:
                # show as integer index when appropriate
                parts.append(f"AQI(index)={int(round(a))} ({LEVEL_NAMES[_aqi_to_level(a)]})")
    else:
        parts.append("AQI=missing")
    if pm25 is not None:
        parts.append(f"PM2.5={pm25} µg/m3 ({LEVEL_NAMES[_pm25_to_level(pm25)]})")
    else:
        parts.append("PM2.5=missing")
    return parts


def _base_recommendations(level: int) -> List[str]:
    # Generic recommendations for each level (ordered by importance)
    if level == 0:  # Normal
        return [
            "Outdoor activities are safe for most people.",
            "No special precautions needed for healthy individuals.",
            "Vulnerable groups may still follow usual care (asthma meds).",
        ]
    if level == 1:  # Moderate
        return [
            "Unusually sensitive people should consider reducing prolonged or heavy exertion outdoors.",
            "Keep windows closed during heavy traffic or dust events.",
            "Consider indoor light activity instead of vigorous outdoor exercise.",
        ]
    if level == 2:  # Unhealthy
        return [
            "Avoid vigorous outdoor exercise; prefer indoor exercise with good filtration.",
            "People with respiratory or cardiovascular conditions should follow their action plans and limit outdoor time.",
            "Consider using an N95/FFP2 mask when outdoors in crowded or polluted areas.",
        ]
    # Hazardous
    return [
        "Stay indoors and keep windows and doors closed where possible.",
        "Use indoor air cleaners (HEPA) or portable filters if available.",
        "Vulnerable people (children, elderly, pregnant, chronic disease) should avoid all outdoor exertion.",
    ]


def interpret(aqi: Optional[float], pm25: Optional[float], timestamp: Optional[str] = None,
              city: Optional[str] = None) -> Dict[str, Any]:
    """
    Interpret AQI and PM2.5 into a risk level, short recommendations and brief explanation.

    Inputs:
      - aqi: numeric AQI (or None)
      - pm25: numeric PM2.5 in µg/m3 (or None)
      - timestamp, city: optional metadata forwarded to output for UI context

    Returns a dict suitable for direct display in UI.
    """
    aqi_level = _aqi_to_level(aqi)
    pm25_level = _pm25_to_level(pm25)

    # Base decision: take the worse pollutant level
    level = max(aqi_level, pm25_level)

    # If one pollutant is elevated and the other is close, be conservative:
    # if difference >= 2, use the higher; if difference == 1, keep the max but
    # note both in reasons (already done).
    # (This rule is simple and explainable.)

    reasons = _reason_strings(aqi, pm25)

    # Start from base recommendations for the computed level
    recs = _base_recommendations(level)

    # Add pollutant-specific adjustments (concise, concrete)
    if pm25_level >= 2:
        # PM2.5-driven: emphasise masks, indoor filtration, avoid line-of-sight outdoor exposure
        recs = [
            r for r in recs
        ]
        if "Consider using an N95/FFP2 mask when outdoors in crowded or polluted areas." not in recs:
            recs.append("Use an N95/FFP2 mask when you must go outside; cloth masks offer limited PM2.5 protection.")
        recs.append("Avoid activities near busy roads, construction, or open burning.")

    if aqi_level >= 2:
        # AQI-driven: emphasise symptoms monitoring and reducing exposure
        if "People with respiratory or cardiovascular conditions should follow their action plans and limit outdoor time." not in recs:
            recs.append("Follow medical action plans if you have asthma or heart disease; keep rescue meds at hand.")

    # Deduplicate and keep 3-6 concise items
    seen = set()
    deduped: List[str] = []
    for r in recs:
        if r not in seen:
            deduped.append(r)
            seen.add(r)
        if len(deduped) >= 6:
            break

    explanation = (
        f"Air quality assessed as {LEVEL_NAMES[level]} based on AQI and PM2.5. "
        f"Higher pollutant determines the risk level; see reasons."
    )

    out: Dict[str, Any] = {
        "risk_level": LEVEL_NAMES[level],
        "score": level,
        "reasons": reasons,
        "recommendations": deduped,
        "explanation": explanation,
    }

    # attach context fields if provided (useful for UI)
    if city:
        out["city"] = city
    if timestamp:
        out["timestamp"] = timestamp

    return out


if __name__ == "__main__":
    # Minimal self-test examples (not unit tests)
    examples = [
        {"aqi": 30, "pm25": 8, "city": "Testville"},
        {"aqi": 85, "pm25": 20, "city": "Town"},
        {"aqi": 120, "pm25": 40, "city": "City"},
        {"aqi": 260, "pm25": 90, "city": "BadAir"},
    ]
    for e in examples:
        print(interpret(e["aqi"], e["pm25"], city=e["city"]))

    # Explicit demonstrations requested by user
    print("--- Example: OpenWeather AQI category = 2 ---")
    print(interpret(2, None, city="ExampleCity"))
    print("--- Example: Numeric AQI index = 120 ---")
    print(interpret(120, None, city="ExampleCity"))
