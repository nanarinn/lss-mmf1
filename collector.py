import argparse
import base64
import json
import logging
from pathlib import Path
import threading
import time
from urllib.request import Request, urlopen
import uuid
import paho.mqtt.client as mqtt
from settings import GROUPS, settings, valid_topic
from storage import save_batch, utcnow
from capture_files import write_capture


def collect(cfg):
    if not cfg['mqtt_password']:
        raise ValueError('Set mqtt_password in config.json or MQTT_PASSWORD in the environment')
    seconds = cfg['capture_seconds']
    if seconds < 1:
        raise ValueError('capture_seconds must be at least 1')
    started = utcnow()
    subscribed = threading.Event()
    errors = []
    latest = {}
    received = 0
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id='lss-api-' + uuid.uuid4().hex[:16], protocol=mqtt.MQTTv311)
    client.username_pw_set(cfg['mqtt_username'], cfg['mqtt_password'])
    client.connect_timeout = 15
    if cfg['mqtt_tls']:
        client.tls_set()

    def on_connect(client, userdata, flags, code, properties):
        if code.is_failure:
            errors.append('MQTT authentication/connection rejected: ' + str(code))
            subscribed.set()
        else:
            result, mid = client.subscribe([('BDNS/' + group + '/#', 1) for group in GROUPS])
            if result != mqtt.MQTT_ERR_SUCCESS:
                errors.append('Could not request subscription')
                subscribed.set()

    def on_subscribe(client, userdata, mid, codes, properties):
        if any(code.is_failure for code in codes):
            errors.append('Topic subscription rejected')
        subscribed.set()

    def on_disconnect(client, userdata, flags, code, properties):
        if code.is_failure:
            errors.append('MQTT disconnected during capture; retry on next refresh')

    def on_message(client, userdata, message):
        nonlocal received
        if not valid_topic(message.topic):
            return
        record = {'topic': message.topic, 'received_at_utc': utcnow(),
                  'qos': message.qos, 'retain': message.retain,
                  'payload_base64': base64.b64encode(message.payload).decode('ascii')}
        try:
            record['payload_text'] = message.payload.decode('utf-8')
            try:
                record['payload_json'] = json.loads(record['payload_text'])
            except ValueError:
                pass
        except UnicodeDecodeError:
            pass
        latest[message.topic] = record
        received += 1

    client.on_connect = on_connect
    client.on_subscribe = on_subscribe
    client.on_message = on_message
    client.on_disconnect = on_disconnect
    running = False
    try:
        client.connect(cfg['mqtt_host'], cfg['mqtt_port'], keepalive=60)
        client.loop_start()
        running = True
        if not subscribed.wait(20):
            raise RuntimeError('MQTT subscription timed out')
        if errors:
            raise RuntimeError('; '.join(errors))
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline and not errors:
            time.sleep(min(0.25, max(0, deadline - time.monotonic())))
    finally:
        client.disconnect()
        if running:
            client.loop_stop()
    if errors:
        raise RuntimeError('; '.join(errors))
    if not latest:
        raise RuntimeError('No MQTT messages received; previous snapshot remains available')
    groups = {group: sum(r['topic'].split('/')[1] == group for r in latest.values()) for group in GROUPS}
    meta = {'started_at_utc': started, 'completed_at_utc': utcnow(),
            'capture_seconds': seconds, 'messages_received': received,
            'topics_updated': len(latest), 'topics_by_group': groups,
            'missing_groups': [group for group, count in groups.items() if not count],
            'mode': 'live_mqtt'}
    return list(latest.values()), meta


def refresh(cfg):
    records, meta = collect(cfg)
    if cfg['push_url']:
        if not cfg['push_url'].startswith('https://') or not cfg['ingest_token']:
            raise ValueError('push_url must be HTTPS and ingest_token must be configured')
        # Keep unsent files across restarts and outages. Each run can drain a
        # backlog while still recording its new sample first.
        write_capture(cfg['outbox_dir'], records, meta)
        for pending in sorted(Path(cfg['outbox_dir']).glob('MMF_*.json'))[:10]:
            queued = json.loads(pending.read_text(encoding='utf-8'))
            allowed = [r for r in queued['records'] if valid_topic(r['topic'])]
            if not allowed:
                # Retain an old out-of-scope capture locally without blocking
                # delivery of new captures for the selected MMF groups.
                pending.replace(pending.with_suffix('.out_of_scope'))
                continue
            if len(allowed) != len(queued['records']):
                queued['records'] = allowed
                queued['metadata'] = dict(queued['metadata'])
                queued['metadata']['topics_updated'] = len(allowed)
                queued['metadata']['topics_by_group'] = {
                    group: sum(r['topic'].split('/')[1] == group for r in allowed) for group in GROUPS}
                queued['metadata']['missing_groups'] = [
                    group for group, count in queued['metadata']['topics_by_group'].items() if not count]
                queued['metadata']['source_messages_received'] = queued['metadata'].pop('messages_received', None)
            req = Request(cfg['push_url'], data=json.dumps(queued).encode('utf-8'), method='POST',
                          headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + cfg['ingest_token']})
            with urlopen(req, timeout=30) as response:
                acknowledgement = json.load(response)
                if response.status != 200 or not acknowledgement.get('capture_file'):
                    raise RuntimeError('Upload not confirmed; snapshot retained locally')
            pending.unlink()
    else:
        save_batch(cfg['db_path'], records, meta)
        write_capture(cfg['capture_dir'], records, meta)
    logging.info('Refreshed %s topics: %s', len(records), meta['topics_by_group'])


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--loop', action='store_true', help='Refresh every 900 seconds until stopped')
    parser.add_argument('--capture-seconds', type=int)
    args = parser.parse_args()
    cfg = settings()
    if args.capture_seconds is not None:
        cfg['capture_seconds'] = args.capture_seconds
    if cfg['refresh_seconds'] < cfg['capture_seconds'] + 30:
        raise ValueError('Refresh interval must exceed capture duration by at least 30 seconds')
    if not args.loop:
        refresh(cfg)
        return
    while True:
        started = time.monotonic()
        try:
            refresh(cfg)
        except Exception:
            logging.exception('Refresh failed; existing data is preserved')
        time.sleep(max(1, cfg['refresh_seconds'] - (time.monotonic() - started)))


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    main()
