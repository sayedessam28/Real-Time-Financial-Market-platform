# Re-export من shared عشان backward compatibility
from shared.schema import market_schema, alert_schema

__all__ = ["market_schema", "alert_schema"]
