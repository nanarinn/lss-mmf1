import json
import os
from pathlib import Path

BASE = Path(__file__).resolve().parent
GROUPS = ('MMF1',)


def settings():
    path = Path(os.environ.get('LSS_CONFIG', str(BASE / 'config.json')))
    data = json.loads(path.read_text(encoding='utf-8')) if path.exists() else {}
    defaults = {'mqtt_host': '139.180.218.77', 'mqtt_port': 1883,
                'mqtt_username': 'tanje', 'mqtt_password': '', 'mqtt_tls': False,
                'capture_seconds': 60, 'refresh_seconds': 900,
                'stale_seconds': 1800, 'read_token': '', 'ingest_token': '',
                'push_url': '', 'db_path': str(BASE / 'data' / 'mmf1.sqlite3'),
                'capture_dir': str(BASE / 'data' / 'captures'),
                'outbox_dir': str(BASE / 'data' / 'outbox')}
    defaults.update(data)
    for key in defaults:
        env = os.environ.get('LSS_' + key.upper())
        if env is not None:
            if isinstance(defaults[key], bool):
                defaults[key] = env.lower() in ('true', '1', 'yes')
            elif isinstance(defaults[key], int):
                defaults[key] = int(env)
            else:
                defaults[key] = env
    if 'MQTT_PASSWORD' in os.environ:
        defaults['mqtt_password'] = os.environ['MQTT_PASSWORD']
    for key in ('db_path', 'capture_dir', 'outbox_dir'):
        path = Path(defaults[key])
        defaults[key] = str(path if path.is_absolute() else BASE / path)
    return defaults


def valid_topic(topic):
    parts = topic.split('/')
    return len(parts) >= 2 and parts[0] == 'BDNS' and parts[1] in GROUPS
