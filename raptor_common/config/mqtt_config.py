from dataclasses import dataclass
from raptor_common.utils.envvars import (EnvVars)

FORMAT_FLAT = "flat-1"
FORMAT_HIER = "hier-1"
FORMAT_LINE_PROTOCOL = "line"


@dataclass(frozen=True)
class MQTTConfig:
    broker: str
    port: int
    username: str
    password: str
    client_id: str
    format: str
    keepalive: int = 60

    def __post_init__(self):
        if not isinstance(self.port, int):
            raise ValueError("Port must be an integer")
        if self.port < 1 or self.port > 65535:
            raise ValueError("Port must be between 1 and 65535")


    @classmethod
    def from_dict(cls, data: dict) -> 'MQTTConfig':
        """Create an MQTTConfig instance from a dictionary"""
        return cls(
            broker=data['broker'],
            port=data['port'],
            username=data['username'],
            password=data['password'],
            client_id=data['client_id'],
            format=data.get('format', FORMAT_FLAT)
        )

    @classmethod
    def get_mqtt_config(cls) -> 'MQTTConfig':
        return cls(
            broker=EnvVars().mqtt_broker,
            port=EnvVars().mqtt_port,
            username=EnvVars().mqtt_root,
            password=EnvVars().mqtt_root_pass,
            client_id=EnvVars().mqtt_client_id,
            format=FORMAT_FLAT
        )
