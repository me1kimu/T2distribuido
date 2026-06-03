from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Literal, Optional, Any, Dict


@dataclass
class QueryRequest:
    # Payload de consultas Q1-Q5 con parametros de zona y confianza.
    query_type: str
    zone_id: Optional[str] = None
    zone_id_a: Optional[str] = None
    zone_id_b: Optional[str] = None
    confidence_min: float = 0.0
    bins: int = 5

    @staticmethod
    def model_validate(payload: Any) -> "QueryRequest":
        if isinstance(payload, QueryRequest):
            return payload
        if isinstance(payload, dict):
            return QueryRequest(
                query_type=payload.get("query_type"),
                zone_id=payload.get("zone_id"),
                zone_id_a=payload.get("zone_id_a"),
                zone_id_b=payload.get("zone_id_b"),
                confidence_min=float(payload.get("confidence_min", 0.0)),
                bins=int(payload.get("bins", 5)),
            )
        raise TypeError("Unsupported payload type for model_validate")

    def model_dump(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class MetricEvent:
    # Eventos de cache para hit/miss y eviccion.
    event_type: str
    query_type: str
    latency_ms: float = 0.0
