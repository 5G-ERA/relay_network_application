import json
import os

import pytest  # pants: no-infer-dep
from rclpy.qos import QoSProfile  # pants: no-infer-dep

from era_5g_relay_network_application.utils import Compressions, load_entities_list

EVAR: str = "TOPICS_TO_SERVER"


def test_load_entities_list() -> None:
    os.environ[EVAR] = ""

    with pytest.raises(ValueError):  # noqa:PT011
        load_entities_list()

    os.environ[EVAR] = json.dumps([])
    assert len(load_entities_list()) == 0

    os.environ[EVAR] = json.dumps([{"name": "whatever"}])
    with pytest.raises(ValueError):  # noqa:PT011
        load_entities_list()

    os.environ[EVAR] = json.dumps([{"name": "whatever", "type": "whatever"}])
    ent = load_entities_list()
    assert len(ent) == 1
    assert ent[0].qos is None
    assert ent[0].compression is None

    os.environ[EVAR] = json.dumps([{"name": "whatever", "type": "whatever", "compression": "lz4"}])
    ent = load_entities_list()
    assert len(ent) == 1
    assert ent[0].qos is None
    assert ent[0].compression == Compressions.LZ4

    os.environ[EVAR] = json.dumps([{"name": "whatever", "type": "whatever", "qos": {"preset": "system_default"}}])
    ent = load_entities_list()
    assert len(ent) == 1
    assert isinstance(ent[0].qos, QoSProfile)
    assert ent[0].compression is None

    os.environ[EVAR] = json.dumps(
        [
            {
                "name": "whatever",
                "type": "whatever",
                "qos": {"depth": 10},
            }
        ]
    )
    ent = load_entities_list()
    assert len(ent) == 1
    assert isinstance(ent[0].qos, QoSProfile)
    assert ent[0].compression is None
