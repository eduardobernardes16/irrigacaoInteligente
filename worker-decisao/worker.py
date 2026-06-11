import asyncio, json, os
import nats
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS

NATS_URL = os.environ.get('NATS_URL', 'nats://nats:4222')
INFLUX_URL = os.environ.get('INFLUX_URL', 'http://influxdb:8086')
INFLUX_TOKEN = os.environ.get('INFLUX_TOKEN', 'irrigacao-token')
INFLUX_ORG = os.environ.get('INFLUX_ORG', 'fazenda')
INFLUX_BUCKET = os.environ.get('INFLUX_BUCKET', 'irrigation')
MOISTURE_THRESHOLD = float(os.environ.get('MOISTURE_THRESHOLD', '30.0'))

influx = InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG)
write_api = influx.write_api(write_options=SYNCHRONOUS)

async def main():
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()

    # Consumer durável — retoma do último ponto processado
    psub = await js.pull_subscribe(
        'sensors.irrigation.*',
        durable='worker-decisao',
        stream='IRRIGATION'
    )
    print(f'Worker de decisão iniciado. Limiar: {MOISTURE_THRESHOLD}%')

    while True:
        try:
            msgs = await psub.fetch(10, timeout=2)
        except nats.errors.TimeoutError:
            continue

        for msg in msgs:
            data = json.loads(msg.data.decode())
            zone = data['zone_id']
            moisture = data['moisture_pct']
            temp = data['temp_soil']
            rain = data.get('rain_forecast', False)

            # Sempre grava leitura no InfluxDB
            point = (Point('soil_readings')
                     .tag('zone_id', zone)
                     .field('moisture_pct', moisture)
                     .field('temp_soil', temp))
            write_api.write(bucket=INFLUX_BUCKET, record=point)

            # Decisão: irrigar se seco E sem previsão de chuva
            if moisture < MOISTURE_THRESHOLD and not rain:
                action = 'LIGAR_BOMBA'
                print(f'[{zone}] IRRIGANDO — moisture={moisture}% < {MOISTURE_THRESHOLD}%')
                action_point = (Point('irrigation_actions')
                                .tag('zone_id', zone)
                                .tag('action', action)
                                .field('moisture_at_trigger', moisture))
                write_api.write(bucket=INFLUX_BUCKET, record=action_point)
                # Publica ação para atuadores (simulados)
                await nc.publish(f'sensors.actions.{zone}',
                                 json.dumps({'zone': zone, 'action': action}).encode())
            elif moisture < MOISTURE_THRESHOLD and rain:
                print(f'[{zone}] Seco mas chuva prevista — não irrigando')
            else:
                print(f'[{zone}] OK — moisture={moisture}%')

            await msg.ack()

if __name__ == '__main__':
    asyncio.run(main())