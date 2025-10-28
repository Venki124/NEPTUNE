# ---- Utility Functions ----

def safe_int(value):
    """Safely convert to int or return None."""
    try:
        return int(value)
    except (TypeError, ValueError):
        return None  

def validate_message(msg,EXPECTED_FIELDS):
    """Validate message keys and types."""
    missing = [f for f in EXPECTED_FIELDS if f not in msg]
    extra = [f for f in msg if f not in EXPECTED_FIELDS]
    type_mismatch = []

    for key, expected_type in EXPECTED_FIELDS.items():
        if key in msg and msg[key] is not None:
            if expected_type == int:
                try:
                    int(msg[key])
                except (ValueError, TypeError):
                    type_mismatch.append(f"{key}: expected int, got {type(msg[key]).__name__}")
            elif not isinstance(msg[key], expected_type):
                type_mismatch.append(f"{key}: expected {expected_type.__name__}, got {type(msg[key]).__name__}")

    return missing, extra, type_mismatch