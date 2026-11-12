import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    nodes: dict[int, str]
    fetch_interval: float
    heartbeat_timeout: float
    election_min: float
    election_max: float
    rpc_timeout: float
    write_timeout: float

    @classmethod
    def load(cls, path: str) -> "Settings":
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        raw["nodes"] = {int(key): value for key, value in raw["nodes"].items()}
        result = cls(**raw)
        if set(result.nodes) != {1, 2, 3, 4} or len(set(result.nodes.values())) != 4:
            raise ValueError("Configure exatamente quatro nos distintos: 1, 2, 3, 4.")
        if not (
            0 < result.fetch_interval < result.heartbeat_timeout
            < result.election_min < result.election_max
            and result.rpc_timeout > 0 and result.write_timeout > 0
        ):
            raise ValueError("Temporizadores invalidos no arquivo de configuracao.")
        return result
