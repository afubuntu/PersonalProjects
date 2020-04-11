import threading
import time
import queue
def consume(q):
    while(True):
        nothing_retrieve=False
        name = threading.currentThread().getName()
        print ("Thread: {0} start get item from queue[current size = {1}] at time = {2} \n".format(name, q.qsize(), time.strftime('%H:%M:%S')))      
        try:
            item = q.get(timeout=2)
        except:
            nothing_retrieve=True

        time.sleep(3)  # spend 3 seconds to process or consume the tiem
        print ("Thread: {0} finish process item from queue[current size = {1}] at time = {2} \n".format(name, q.qsize(), time.strftime('%H:%M:%S')))
        if not nothing_retrieve:
            q.task_done()
        print('The remaining number of tasks is {}'.format(q.unfinished_tasks))
        if(q.unfinished_tasks<=0):
            if q.empty():
                break 
 
def producer(q):
    # the main thread will put new items to the queue
 
    for i in range(10):
        name = threading.currentThread().getName()
        print ("Thread: {0} start put item into queue[current size = {1}] at time = {2} \n".format(name, q.qsize(), time.strftime('%H:%M:%S')))
        item = "item-" + str(i)
        q.put(item)
        print ("Thread: {0} successfully put item into queue[current size = {1}] at time = {2} \n".format(name, q.qsize(), time.strftime('%H:%M:%S')))
 
    q.join()
 
if __name__ == '__main__':
    q = queue.Queue(maxsize = 3)
 
    threads_num = 3  # three threads to consume
    threads=[]
    print('The number of active threads is : {}'.format(threading.active_count()))
    for i in range(threads_num):
        t = threading.Thread(name = "ConsumerThread-"+str(i), target=consume, args=(q,))
        t.start()
        threads.append(t)
 
    #1 thread to procuce
    t = threading.Thread(name = "ProducerThread", target=producer, args=(q,))
    t.start()
    threads.append(t)
    print('The number of active threads is : {}'.format(threading.active_count()))
    q.join()

    for t in threads:
        print('Thread[{}] is active=>{}'.format(t.name,t.is_alive()))
        t.join()
    #print('The number of active threads is : {}'.format(threading.active_count()))
    #print('End of the main Thread...')

    #time.sleep(20)

    for t in threads:
        print('Thread[{}] is active=>{}'.format(t.name,t.is_alive()))
    print('The number of active threads is : {}'.format(threading.active_count()))
    print('End of the main Thread after 20 seconds...')
