from qpython import qaioconnection
import asyncio
import time

# PYTHONPATH="/home/brian/python/qPython"
# q -p 5000
# .z.pg:{[x]0N!(`zpg;x);value x}


async def main_func():
    start_time = time.time()
    # create connection object
    q = qaioconnection.QConnection(host='localhost', port=5000)
    # initialize connection
    await q.open()

    print(q)
    print('IPC version: %s. Is connected: %s' % (q.protocol_version, q.is_connected()))

    # simple query execution via: QConnection.__call__
    data = await q('{`int$ til x}', 10)
    print(data)
    print('type: %s, numpy.dtype: %s, meta.qtype: %s, data: %s ' % (type(data), data.dtype, data.meta.qtype, data))

    # simple query execution via: QConnection.sendSync
    data = await q.sendSync('{`long$ til x}', 10)
    print('type: %s, numpy.dtype: %s, meta.qtype: %s, data: %s ' % (type(data), data.dtype, data.meta.qtype, data))

    # low-level query and read
    # sends a SYNC query
    await q.query(qaioconnection.MessageType.SYNC, '{`short$ til x}', 10)
    # retrieve entire message
    msg = await q.receive(data_only=False, raw=False)
    print('type: %s, message type: %s, data size: %s' % (type(msg), msg.type, msg.size))
    data = msg.data
    print('type: %s, numpy.dtype: %s, meta.qtype: %s, data: %s ' % (type(data), data.dtype, data.meta.qtype, data))
    # close connection
    await q.close()
    end_time = time.time()
    print(end_time - start_time)


async def kdb_query(kdb_port, query):
    start_time = time.time()
    q = qaioconnection.QConnection(host='localhost', port=kdb_port)
    await q.open()
    print(q)

    data = await q.sendSync(query)
    #print(data)

    await q.close()
    print(f"closed port {kdb_port}")
    end_time = time.time()
    print(f"{end_time - start_time}")


async def wrapper(kdb_port, work_queue):
    while not work_queue.empty():
        query = await work_queue.get()
        await kdb_query(kdb_port, query)


async def main():
    start_time = time.time()
    work_queue = asyncio.Queue()

    for query in [
        "select avg price from t where sym=`AAPL",
        "select from t where sym=`TWTR",
        "select from t where sym=`META",
        "select avg price from t where sym=`AAPL",
    ]:
        await work_queue.put(query)

    # Run the tasks
    await asyncio.gather(
        asyncio.create_task(wrapper(5000, work_queue)),
        asyncio.create_task(wrapper(5001, work_queue)),
    )

    end_time = time.time()
    print(f"{end_time - start_time}")


if __name__ == '__main__':
    asyncio.run(main())
