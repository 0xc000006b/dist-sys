import hazelcast
import threading
import time
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("no-lock-increment")

def setup_hz_client():
    logger.info("Підключення до кластера Hazelcast")
    return hazelcast.HazelcastClient(
        cluster_name="my-cluster",
        cluster_members=["host.docker.internal:5701"],
    )

def update_counter(dist_map, iterations):
    for _ in range(iterations):
        value = dist_map.get("total") or 0
        dist_map.set("total", value + 1)

def execute_no_lock_test(num_iterations=10000, num_threads=3):
    client = setup_hz_client()
    try:
        dist_map = client.get_map("new_map").blocking()
        dist_map.set("total", 0)
        expected_total = num_iterations * num_threads
        logger.info(f"Тест: {num_threads} потоків, {num_iterations} ітерацій на потік")
        start_time = time.perf_counter()
        workers = []
        for i in range(num_threads):
            worker = threading.Thread(
                target=update_counter,
                args=(dist_map, num_iterations),
                name=f"Updater-{i+1}"
            )
            workers.append(worker)
            worker.start()

        for worker in workers:
            worker.join()

        elapsed_time = time.perf_counter() - start_time
        final_total = dist_map.get("total")

        logger.info(f"Отриманий результат: {final_total}, Очікуваний: {expected_total}")
        logger.info(f"Час виконання: {elapsed_time:.2f} секунд")
        if final_total < expected_total:
            logger.warning("Втрата даних через конкурентний доступ")

    finally:
        logger.info("Закриття клієнта Hazelcast")
        client.shutdown()

if __name__ == "__main__":
    logger.info("Розпочато тестування інкременту без блокувань")
    execute_no_lock_test()
    logger.info("Тест завершено")
