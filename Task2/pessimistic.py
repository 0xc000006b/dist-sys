import hazelcast
import threading
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("pessimistic-test")

def create_hz_client():
    logger.info("Ініціалізація клієнта Hazelcast")
    return hazelcast.HazelcastClient(
        cluster_name="my-cluster",
        cluster_members=["localhost:5701"],
        smart_routing=True
    )

def safe_increment(map_instance, num_ops):
    for _ in range(num_ops):
        map_instance.lock("result")
        try:
            val = map_instance.get("result") or 0
            map_instance.set("result", val + 1)
        finally:
            map_instance.unlock("result")

def test_pessimistic_increment(iterations=1000, threads=3):
    client = create_hz_client()
    try:
        hz_map = client.get_map("counter_map").blocking()
        hz_map.set("result", 0)
        expected = iterations * threads
        logger.info(f"Тест: {threads} потоків, {iterations} ітерацій на потік, очікуваний результат: {expected}")
        members = client.cluster_service.get_members()
        logger.info(f"Кластер: {len(members)} нод ({[str(m) for m in members]})")

        start = time.perf_counter()

        worker_threads = []
        for i in range(threads):
            t = threading.Thread(
                target=safe_increment,
                args=(hz_map, iterations),
                name=f"Safe-Increment-{i+1}"
            )
            worker_threads.append(t)
            t.start()

        for t in worker_threads:
            t.join()

        elapsed = time.perf_counter() - start
        final_val = hz_map.get("result")

        logger.info(f"Результат: {final_val}, Очікувано: {expected}, Час: {elapsed:.2f} сек")
        if final_val == expected:
            logger.info("Тест пройдено успішно")
        else:
            logger.error(f"Помилка: результат {final_val} не відповідає {expected}")

    finally:
        logger.info("Закриття клієнта")
        client.shutdown()

if __name__ == "__main__":
    logger.info("Початок тесту песимістичного блокування")
    test_pessimistic_increment(iterations=10000)
    logger.info("Тест завершено")
