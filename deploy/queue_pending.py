# -*- coding: utf-8 -*-
"""Очередь запросов при PoolError — краткосрочная память, БД молотит по мере освобождения."""
import asyncio
import logging
import queue as queue_module

log = logging.getLogger(__name__)

# (update, target_update_queue) — куда вернуть для повторной обработки
_pending: asyncio.Queue = asyncio.Queue()
_PROCESS_INTERVAL = 10  # секунд
_INTERVAL_WHEN_BUSY = 15  # при большой очереди — реже, чтобы не перегружать БД


def add_pending(update, target_queue):
    """Добавить запрос в очередь при исчерпании пула БД."""
    try:
        _pending.put_nowait((update, target_queue))
        log.info("Очередь: +1 (всего ~%d)", _pending.qsize())
    except Exception as e:
        log.warning("Очередь add: %s", e)


async def _process_pending_worker():
    """Раз в N секунд возвращаем один запрос в обработку — БД не перегружается."""
    while True:
        qsize = _pending.qsize()
        # При большой очереди — реже отдаём, чтобы БД не крашилась
        interval = _INTERVAL_WHEN_BUSY if qsize > 5 else _PROCESS_INTERVAL
        await asyncio.sleep(interval)
        try:
            if _pending.empty():
                continue
            update, target_queue = _pending.get_nowait()
            await target_queue.put(update)
            log.info("Очередь: отправлен на обработку (осталось ~%d)", _pending.qsize())
        except queue_module.Empty:
            pass
        except Exception as e:
            log.warning("Очередь process: %s", e)


def start_pending_processor():
    """Запустить фоновую задачу обработки очереди."""
    return asyncio.create_task(_process_pending_worker())
