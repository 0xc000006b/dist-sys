import hazelcast
import threading
import time

client = hazelcast.HazelcastClient(
    cluster_name="my-cluster",
    cluster_members=["localhost:5701", "localhost:5702", "localhost:5703"]
)

queue = client.get_queue("bounded-queue-test").blocking()

producer_done = False
stop_put_task = False

def writer():
    global producer_done
    print("Продюсер почав запис значень від 1 до 100")
    for i in range(1, 101):
        queue.put(i)
        print(f"Продюсер записав: {i}, Розмір черги: {queue.size()}")
        time.sleep(0.05)
    print("Продюсер завершив запис")
    producer_done = True


def reader(reader_id):
    print(f"Консюмер {reader_id} почав читання")
    while not producer_done or queue.size() > 0:
        try:
            item = queue.poll(timeout=1.0)
            if item is None:
                if producer_done and queue.size() == 0:
                    break
                continue
            print(f"Консюмер {reader_id} прочитав: {item}, Розмір черги: {queue.size()}")
        except Exception as e:
            print(f"Помилка Консюмера {reader_id}: {e}")
            break
    print(f"Консюмер {reader_id} завершив читання")


def attempt_put(queue):
    global stop_put_task
    try:
        queue.put(11)
        print("WARNING: 11-й елемент додано")
    except Exception as e:
        if not stop_put_task:
            print(f"Помилка при спробі запису: {e}")


def test_queue_limit():
    global stop_put_task
    print("\n == Тестування поведінки при заповненій черзі без консюмерів ==")
    queue.clear()
    print("Чергу очищено")

    for i in range(1, 11):
        queue.put(i)
        print(f"Продюсер записав для тесту: {i}, Розмір черги: {queue.size()}")
    print("Спроба записати 11-й елемент (черга повна)...")
    start_time = time.time()

    put_task = threading.Thread(target=attempt_put, args=(queue,))
    put_task.daemon = True
    put_task.start()

    time.sleep(5)
    elapsed = time.time() - start_time
    if put_task.is_alive():
        print(f"Заблоковано на {elapsed:.2f} секунд, як очікувалося")
    else:
        print("WARNING: запис не заблоковано")

    stop_put_task = True

if __name__ == "__main__":
    print("== Початок демонстрації продюсера та консюмерів ==")
    writer_thread = threading.Thread(target=writer, name="Writer")
    reader1_thread = threading.Thread(target=reader, args=(1,), name="Reader-1")
    reader2_thread = threading.Thread(target=reader, args=(2,), name="Reader-2")
    writer_thread.start()
    reader1_thread.start()
    reader2_thread.start()

    writer_thread.join()
    reader1_thread.join()
    reader2_thread.join()
    test_queue_limit()

    client.shutdown()
    print("\n== Тест завершено ==")
