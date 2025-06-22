import hazelcast
import logging

# Налаштування логування
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("map-demo")

def init_hz_client():
    """Створює клієнт Hazelcast."""
    return hazelcast.HazelcastClient(
        cluster_name="my-cluster",
        cluster_members=["host.docker.internal:5701"],
        connection_timeout=10.0,
        heartbeat_interval=5.0,
        heartbeat_timeout=60.0
    )

def populate_map():
    """Заповнює мапу 1000 значеннями."""
    client = init_hz_client()
    try:
        hz_map = client.get_map("my-distributed-map").blocking()
        logger.info("Починаємо запис 1000 елементів у мапу")

        for idx in range(1000):
            hz_map.set(f"id-{idx}", f"data-{idx}")

        total_size = hz_map.size()
        logger.info(f"Успішно записано. Розмір мапи: {total_size}")
        return total_size == 1000
    finally:
        client.shutdown()

if __name__ == "__main__":
    logger.info("Запуск демонстрації Distributed Map")
    success = populate_map()
    logger.info(f"Тест пройдено: {success}")
