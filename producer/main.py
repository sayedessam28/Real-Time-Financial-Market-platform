from threading import Thread
from producer.producers.usdegp_producer import start_usdegp_stream
from producer.producers.gold_producer import start_gold_producer
from producer.producers.local_gold_producer import start_local_gold_producer

if __name__ == "__main__":
    t1 = Thread(target=start_usdegp_stream)
    t2 = Thread(target=start_gold_producer)
    t3 = Thread(target=start_local_gold_producer)

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()