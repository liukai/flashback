import unittest
import queue
import threading
import time
import pprint


def gen_infinite_1():
    while True:
        yield 1


def gen_seq(limit=0):
    num = 0
    while limit == 0 or limit > num:
        yield num
        num += 1


class QueueTest(unittest.TestCase):

    def test_create_empty_queue(self):
        with queue.make_queue([], batch_size=100) as dummy_queue:
            pass

        with queue.make_queue(gen_infinite_1(), batch_size=100) as q:
            self.assertFalse(q.should_stop)
            q.stop()
            self.assertTrue(q.should_stop)

        # a reference to the `q` object
        q_ref = None
        with queue.make_queue(gen_infinite_1(), batch_size=100) as q:
            q_ref = q
        self.assertTrue(q_ref.should_stop)

    def test_limited_items_with_n_threads(self):
        thread_num = 5
        limit = 10000
        batch_size = 100
        with queue.make_queue(gen_seq(limit), batch_size) as q:
            items_list = []
            threads = []
            for dummy_idx in xrange(thread_num):
                items_list.append([])
                threads.append(self._start_fetching_thread(q, items_list[-1]))
            self.assertFalse(q.should_stop)

            for t in threads:
                t.join()
            pprint.pprint(q.loading_stats.__dict__)
            pprint.pprint(q.reading_stats.__dict__)

        for idx in xrange(thread_num):
            items = items_list[idx]
            self.assertGreater(len(items), 0)
            total_item_size = sum(len(items) for items in items_list)
            # items are sorted
            self.assertTrue(
                all(items[i] < items[i + 1]
                    for i in xrange(len(items) - 1))
            )
        self.assertEqual(total_item_size,
                         q.loading_stats.num_items_loaded)
        self.assertEqual(q.reading_stats.num_items_read,
                         q.loading_stats.num_items_loaded)

    def test_unlimited_items_with_n_threads(self):
        thread_num = 5
        batch_size = 100
        q_ref = None
        with queue.make_queue(gen_seq(), batch_size) as q:
            q_ref = q
            items_list = []
            threads = []
            for dummy_idx in xrange(thread_num):
                items_list.append([])
                threads.append(self._start_fetching_thread(q, items_list[-1]))
            time.sleep(1)

        for t in threads:
            t.join()

        for idx in xrange(thread_num):
            items = items_list[idx]
            self.assertGreater(len(items), 0)
            total_item_size = sum(len(items) for items in items_list)
            # items are sorted
            self.assertTrue(
                all(items[i] < items[i + 1] for i in xrange(len(items) - 1))
            )
        pprint.pprint(q_ref.loading_stats.__dict__)
        pprint.pprint(q_ref.reading_stats.__dict__)
        self.assertEqual(total_item_size,
                         q.reading_stats.num_items_read)
        self.assertLessEqual(q.reading_stats.num_items_read,
                             q.loading_stats.num_items_loaded)
        # some items are not read because of the abrupt halt, at most
        # 2 * batch_size items (in active and next batches) can be unread.
        self.assertGreaterEqual(
            q.reading_stats.num_items_read + 2 * batch_size,
            q.loading_stats.num_items_loaded)

    def _start_fetching_thread(self, preloading_queue, items):
        def fetch():
            for item in iter(preloading_queue):
                items.append(item)

        thread = threading.Thread(target=fetch)
        thread.start()
        return thread


if __name__ == '__main__':
    unittest.main()
