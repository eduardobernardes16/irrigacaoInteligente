import asyncio, json, random, os
import nats

NATS_URL = os.environ.get('NATS_URL', 'nats://nats:4222')
ZONES = ['zona-1', 'zona-2', 'zona-3', 'zona-4']
RAIN_FORECAST = os.environ.get('RAIN_FORECAST', 'false').lower() == 'true'

# Estado simulado: zonas ficam secando ao longo do tempo
zone_moisture = {z: random.uniform(20.0, 80.0) for z in ZONES}

async def main():
    nc = await nats.connect(NATS_URL)
    js = nc.jetstream()
    # Criar stream se não existir
    try:
        await js.add_stream(name='IRRIGATION', subjects=['sensors.irrigation.>'])
    except Exception:
        pass  # já existe

    print(f'Gerador iniciado. RAIN_FORECAST={RAIN_FORECAST}')
    while True:
        for zone in ZONES:
            # Simula secagem gradual (perde 0-2% por ciclo)
            zone_moisture[zone] = max(5.0, zone_moisture[zone] - random.uniform(0, 2.0))
            # Quando irrigado (simulado externamente), umidade sobe
            if random.random() < 0.05:  # 5% chance de chuva/irrigação
                zone_moisture[zone] = min(95.0, zone_moisture[zone] + random.uniform(20, 40))
            reading = {
                'zone_id': zone,
                'moisture_pct': round(zone_moisture[zone], 1),
                'temp_soil': round(random.uniform(18.0, 35.0), 1),
                'rain_forecast': RAIN_FORECAST
            }
            subject = f'sensors.irrigation.{zone}'
            await js.publish(subject, json.dumps(reading).encode())
            print(f'[{zone}] moisture={reading["moisture_pct"]}% temp={reading["temp_soil"]}°C')
        await asyncio.sleep(3)

if __name__ == '__main__':
    asyncio.run(main())