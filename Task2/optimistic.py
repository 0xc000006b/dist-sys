import hazelcast
import threading
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("optimistic-test")

def create_hz_client():
    logger.info("Ініціалізація клієнта Hazelcast")
    return hazelcast.HazelcastClient(
        cluster_name="my-cluster",
        cluster_members=["localhost:5701"],
        smart_routing=True
    )

def atomic_increment(map_instance, num_ops):
    for _ in range(num_ops):
        while True:
            current_val = map_instance.get("total") or 0
            next_val = current_val + 1
            if map_instance.replace_if_same("total", current_val, next_val):
                break

def test_optimistic_increment(iterations=1000, threads=3):
    client = create_hz_client()
    try:
        hz_map = client.get_map("new_map").blocking()
        hz_map.set("total", 0)

        expected = iterations * threads
        logger.info(f"Тест: {threads} потоків, {iterations} ітерацій на потік, очікуваний результат: {expected}")

        members = client.cluster_service.get_members()
        logger.info(f"Кластер: {len(members)} нод ({[str(m) for m in members]})")

        start = time.perf_counter()

        worker_threads = []
        for i in range(threads):
            t = threading.Thread(
                target=atomic_increment,
                args=(hz_map, iterations),
                name=f"Atomic-Increment-{i+1}"
            )
            worker_threads.append(t)
            t.start()

        for t in worker_threads:
            t.join()

        elapsed = time.perf_counter() - start
        final_val = hz_map.get("total")

        logger.info(f"Результат: {final_val}, Очікувано: {expected}, Час: {elapsed:.2f} сек")
        if final_val == expected:
            logger.info("Тест пройдено успішно")
        else:
            logger.warning(f"Результат {final_val} не дорівнює {expected} через конкуренцію")

    finally:
        logger.info("Закриття клієнта")
        client.shutdown()

if __name__ == "__main__":
    logger.info("Початок тесту оптимістичного блокування")
    test_optimistic_increment(iterations=10000)
    logger.info("Тест завершено")
