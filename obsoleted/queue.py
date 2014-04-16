import utils
import threading
from contextlib import contextmanager
from datetime import datetime
from datetime import timedelta


class PreloadingQueue(object):

    @staticmethod
    def make_batch(batch_size):
        batch = utils.EmptyClass()
        batch.pos = 0
        batch.size = 0
        # Pre-populate the list to avoid resizing.
        batch.items = [None] * batch_size

        return batch

    @staticmethod
    def make_loading_stats():
        stats = utils.EmptyClass()
        stats.num_items_loaded = 0
        stats.total_loading_time = timedelta()
        stats.total_waiting_time = timedelta()
        return stats

    @staticmethod
    def make_reading_stats():
        stats = utils.EmptyClass()
        stats.total_waiting_time = timedelta()
        stats.num_items_read = 0
        return stats

    def __init__(self, source, batch_size):
        self.lock = threading.Lock()
        self.queue_not_empty = threading.Condition(self.lock)
        self.ok_to_load_next_batch = threading.Condition(self.lock)

        self.loading_stats = PreloadingQueue.make_loading_stats()
        self.reading_stats = PreloadingQueue.make_reading_stats()
        self.source = source
        self.max_batch_size = batch_size
        self.should_stop = False
        self.active_batch = self._make_loaded_batch()
        self.next_batch = None

        # Kick off a background thread that keeps pre-loading items.
        self.loading_thread = threading.Thread(target=self._fill_queue)
        self.loading_thread.setDaemon(True)
        self.loading_thread.start()

    def stop(self):
        """Stop can be called multiple times"""
        self.lock.acquire()
        try:
            self.should_stop = True
            self.ok_to_load_next_batch.notify_all()
            self.queue_not_empty.notify_all()
        finally:
            self.lock.release()
        self.loading_thread.join()

    def dequeue(self):
        self.lock.acquire()
        try:
            if self.should_stop or self.active_batch.size == 0:
                return None

            # Already reached the last element?
            if self.active_batch.pos >= self.active_batch.size:
                assert self.active_batch.pos == self.active_batch.size
                # wait for next batch to be loaded

                # TODO this is "the" most hard to read condition!
                # Explain the shit!
                while self.active_batch.size != 0 \
                    and self.active_batch.pos >= self.active_batch.size \
                    and self.next_batch is None \
                    and not self.should_stop:
                    start_time = datetime.now()
                    self.queue_not_empty.wait(1)
                    self.reading_stats.total_waiting_time += \
                        datetime.now() - start_time

                # if the loading thread has already loaded everything,
                # self.should_stop will be set to True.
                if self.should_stop or self.active_batch.size == 0:
                    return None

                # it is also likely that other blocked threads replaced the
                # block.
                if self.active_batch.pos >= self.active_batch.size:
                    assert self.next_batch is not None
                    self.active_batch = self.next_batch
                    self.active_batch.pos = 0
                    self.next_batch = None
                    self.ok_to_load_next_batch.notify_all()
                    self.queue_not_empty.notify_all()

                if self.active_batch.size == 0:
                    return None

            assert self.active_batch.size > 0
            item = self.active_batch.items[self.active_batch.pos]
            self.active_batch.items[self.active_batch.pos] = None
            self.active_batch.pos += 1
            self.reading_stats.num_items_read += 1
            return item
        finally:
            self.lock.release()

    def __iter__(self):
        def gen_dequeue():
            while True:
                item = self.dequeue()
                if item is not None:
                    yield item
                else:
                    break

        return gen_dequeue()

    def _make_loaded_batch(self):
        utils.LOG.info("current log id %d", threading.current_thread(). ident)
        start_time = datetime.now()
        batch = PreloadingQueue.make_batch(self.max_batch_size)
        utils.LOG.info("start loading...")
        for item in self.source:
            batch.items[batch.size] = item
            batch.size += 1

            if batch.size == self.max_batch_size:
                break
        utils.LOG.info("finished loading %d items", batch.size)
        self.loading_stats.num_items_loaded += batch.size
        self.loading_stats.total_loading_time += (datetime.now() - start_time)

        return batch

    def _fill_queue(self):
        """constantly detect and when a batch is consumed, fill the next batch
        with items."""
        try:
            while True:
                self.lock.acquire()

                while self.next_batch is not None and not self.should_stop:
                    start_time = datetime.now()
                    self.ok_to_load_next_batch.wait(1)
                    self.loading_stats.total_waiting_time += \
                        datetime.now() - start_time

                self.lock.release()
                if self.should_stop:
                    break

                # yield lock and load next batch
                next_batch = self._make_loaded_batch()

                self.lock.acquire()
                self.next_batch = next_batch
                self.queue_not_empty.notify_all()
                self.lock.release()

                # If nothing loaded, we shall stop and notify all waiting
                # threads that they should stop.
                if next_batch.size == 0:
                    utils.LOG.info("All items loaded.")
                    break
        finally:
            # Tell all waiting threads that game's over
            self.lock.acquire()
            self.queue_not_empty.notify_all()
            self.lock.release()


@contextmanager
def make_queue(source, batch_size):
    """Return a PreloadingQueue object that works with "with" statement."""
    queue = None
    try:
        queue = PreloadingQueue(source, batch_size)
        yield queue
    finally:
        if queue is not None:
            queue.stop()
