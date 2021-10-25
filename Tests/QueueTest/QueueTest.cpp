//#############################################################################
//# QueueTest (c) Andreas Foedrowitz
//# 2021-jun-20 : File created
//#############################################################################

#include <atomic>
#include <iostream>
#include <list>
#include <pthread.h>
#include <unistd.h>

//#############################################################################

class NoLock
{
public:
    NoLock()
    {
    }
    inline bool lock()
    {
        return true;
    }
    inline bool unlock()
    {
        return true;
    }
};

class Mutex
{
public:
    Mutex()
    {
        pthread_mutexattr_init(&m_attr);
        pthread_mutexattr_settype(&m_attr, PTHREAD_MUTEX_NORMAL);
        pthread_mutex_init(&m_handle, &m_attr);
    }
    ~Mutex()
    {
        pthread_mutex_destroy(&m_handle);
        pthread_mutexattr_destroy(&m_attr);
    }
    inline bool lock()
    {
        return (pthread_mutex_lock(&m_handle) == 0);
    }
    inline bool unlock()
    {
        return (pthread_mutex_unlock(&m_handle) == 0);
    }

private:
    pthread_mutex_t m_handle = { };
    pthread_mutexattr_t m_attr = { };
};

class SpinLock
{
public:
    SpinLock()
    {
    }
    ~SpinLock()
    {
    }
    inline bool lock()
    {
        while (++m_lock != 1) --m_lock;
        return true;
    }
    inline bool unlock()
    {
        --m_lock;
        return false;
    }

private:
    std::atomic<int> m_lock = { };
};

//#############################################################################

class Thread
{
public:
    Thread()
    {
        pthread_attr_init(&m_attr);
        pthread_create(&m_thread, &m_attr, &func, static_cast<void*>(this));
    }
    ~Thread()
    {
        stop();
        pthread_attr_destroy(&m_attr);
    }
    bool stop()
    {
        return (pthread_join(m_thread, nullptr) == 0);
    }
    inline bool started() const
    {
        return m_started;
    }

protected:
    virtual void run() = 0;

private:
    pthread_t m_thread = { };
    pthread_attr_t m_attr = { };
    std::atomic<bool> m_started = { };

    static void* func(void *arg)
    {
        Thread *t = static_cast<Thread*>(arg);
        t->m_started = true;
        t->run();
        return nullptr;
    }

};

//#############################################################################

template<class T>
class StdListQueue
{
public:
    StdListQueue(const size_t capacity) : m_capacity(capacity)
    {
    }
    inline bool write(const T& t)
    {
        m_mutex.lock();
        if (m_list.size() < m_capacity) {
            m_list.push_back(t);
            m_mutex.unlock();
            return true;
        }
        m_mutex.unlock();
        return false;
    }
    inline bool read(T &t)
    {
        m_mutex.lock();
        if (m_list.size() > 0) {
            t = m_list.front();
            m_list.pop_front();
            m_mutex.unlock();
            return true;
        }
        m_mutex.unlock();
        return false;
    }
    inline size_t size() const
    {
        m_mutex.lock();
        size_t s = m_list.size();
        m_mutex.unlock();
        return s;
    }

private:
    std::list<T> m_list;
    size_t m_capacity = 0;
    mutable Mutex m_mutex;
};

//#############################################################################

template<class T, class TLock>
class LockedQueue
{
public:
    LockedQueue(const size_t capacity) : m_capacity(capacity)
    {
        m_buffer = new T[m_capacity];
        for (size_t idx = 0; idx < m_capacity; ++idx) m_buffer[idx] = 0;
    }
    ~LockedQueue()
    {
        delete[] m_buffer;
    }
    inline bool write(const T& t)
    {
        m_lock.lock();
        if (++m_size > m_capacity) {
            --m_size;
            m_lock.unlock();
            return false;
        }
        const size_t idx = m_widx;
        m_widx = (m_widx + 1) % m_capacity;
        m_buffer[idx] = t;
        m_lock.unlock();
        return true;
    }
    inline bool read(T &t)
    {
        m_lock.lock();
        if (m_size > 0) {
            const size_t idx = m_ridx;
            m_ridx = (m_ridx + 1) % m_capacity;
            t = m_buffer[idx];
            --m_size;
            m_lock.unlock();
            return true;
        }
        m_lock.unlock();
        return false;
    }
    inline size_t size() const
    {
        m_lock.lock();
        const size_t s = std::min(m_capacity, m_size);
        m_lock.unlock();
        return s;
    }

private:
    T* m_buffer = 0;
    size_t m_capacity = 0;
    size_t m_size = 0;
    size_t m_widx = 0;
    size_t m_ridx = 0;
    mutable TLock m_lock;
};

template<class T>
class NoLockQueue: public LockedQueue<T, NoLock> // not thread safe, won't crash but won't fully work as expected either
{
public:
    NoLockQueue(const size_t capacity) : LockedQueue<T, NoLock>(capacity)
    {
    }
};

template<class T>
class MutexQueue: public LockedQueue<T, Mutex>
{
public:
    MutexQueue(const size_t capacity) : LockedQueue<T, Mutex>(capacity)
    {
    }
};

template<class T>
class SpinQueue: public LockedQueue<T, SpinLock>
{
public:
    SpinQueue(const size_t capacity) : LockedQueue<T, SpinLock>(capacity)
    {
    }
};

//#############################################################################

template<class T>
class LFQueue
{
public:
//#define MemOrder __ATOMIC_RELAXED
#define MemOrder __ATOMIC_SEQ_CST
    LFQueue(const size_t capacity) : m_capacity(capacity)
    {
        if (__builtin_popcount(m_capacity) != 1) { // no power of 2 ?
            m_capacity = 1LL << (64 - __builtin_clzl(m_capacity)); // round up to next power of 2
        }
        m_capacityMask = m_capacity - 1; // create bitmask with all lower bits set
        m_buffer = new T[m_capacity];
        m_stat = new uint8_t[m_capacity];
        for (size_t idx = 0; idx < m_capacity; ++idx) m_stat[idx] = 0;
    }
    ~LFQueue()
    {
        delete[] m_stat;
        delete[] m_buffer;
    }
    inline bool write(const T& t)
    {
        if (__atomic_add_fetch(&m_size, 1, MemOrder) > m_capacity) {
            __atomic_sub_fetch(&m_size, 1, MemOrder);
            return false;
        }
        // the order of the following 2 lines can be swapped without generating a difference, so __ATOMIC_RELAXED is fine
        const size_t idx = __atomic_fetch_add(&m_widx, 1, MemOrder) & m_capacityMask;
        __atomic_and_fetch(&m_widx, m_capacityMask, MemOrder);
        m_buffer[idx] = t;
        __atomic_store_n(&m_stat[idx], 1, MemOrder);
        return true;
    }
    inline bool read(T &t)
    {
        if (__atomic_load_n(&m_size, MemOrder) > 0) {
            while (!__atomic_load_n(&m_stat[m_ridx], MemOrder)) {

            }
            t = m_buffer[m_ridx];
            __atomic_store_n(&m_stat[m_ridx], 0, MemOrder);
            __atomic_sub_fetch(&m_size, 1, MemOrder); // we have saved the value, so its slot can be released
            m_ridx = (m_ridx + 1) & m_capacityMask; // only a single reader, so no atomic operations needed
            return true;
        }
        return false;
    }
    inline size_t size() const
    {
        return std::min(m_capacity, __atomic_load_n(const_cast<decltype(m_size)*>(&m_size), MemOrder));
    }

private:
    T* m_buffer = 0;
    uint8_t *m_stat = 0;
    size_t m_capacity = 0;
    size_t m_capacityMask = 0;
    size_t m_size = 0;
    size_t m_widx = 0;
    size_t m_ridx = 0;
};

//#############################################################################

template<class T>
class LFAQueue
{
public:
    LFAQueue(const size_t capacity) : m_capacity(capacity)
    {
        if (__builtin_popcount(m_capacity) != 1) { // no power of 2 ?
            m_capacity = 1LL << (64 - __builtin_clzl(m_capacity)); // round up to next power of 2
        }
        m_capacityMask = m_capacity - 1; // create bitmask with all lower bits set
        m_buffer = new T[m_capacity];
        m_stat = new std::atomic<bool>[m_capacity];
        for (size_t idx = 0; idx < m_capacity; ++idx) m_stat[idx] = false;
    }
    ~LFAQueue()
    {
        delete[] m_buffer;
        delete[] m_stat;
    }
    inline bool write(const T& t)
    {
        if (++m_size > m_capacity) {
            --m_size;
            return false;
        }
        // the order of the following 2 lines can be swapped without generating a difference, so __ATOMIC_RELAXED is fine
        const size_t idx = m_widx++ & m_capacityMask;
        m_widx &= m_capacityMask;
        m_buffer[idx] = t;
        m_stat[idx] = true;
        return true;
    }
    inline bool read(T &t)
    {
        if (m_size > 0) {
            while (!m_stat[m_ridx]) {
                 // spin lock
            }
            t = m_buffer[m_ridx];
            m_stat[m_ridx] = false;
            --m_size; // we have saved the value, so its slot can be released
            m_ridx = (m_ridx + 1) & m_capacityMask; // only a single reader, so no atomic operations needed
            return true;
        }
        return false;
    }
    inline size_t size() const
    {
        return std::min<size_t>(m_capacity, m_size);
    }

private:
    T* m_buffer = 0;
    std::atomic<bool> *m_stat = 0;
    size_t m_capacity = 0;
    size_t m_capacityMask = 0;
    std::atomic<size_t> m_size = { };
    std::atomic<size_t> m_widx = { };
    size_t m_ridx = { };
};

//#############################################################################

constexpr static int NumProducers = 10;
constexpr static size_t QueueCapacity = 0x1000;
constexpr static int64_t RunTimeInSeconds = 10;
constexpr static int64_t PayloadSize = 0x100000;

std::atomic<bool> g_running = { };

//#############################################################################

class Item
{
public:
    Item()
    {
    }
    Item(const int64_t channel) : m_channel(channel)
    {
    }
    inline void newVal()
    {
        ++m_val;
    }
    inline int64_t channel() const
    {
        return m_channel;
    }
    inline int64_t val() const
    {
        return m_val;
    }
private:
    int64_t m_channel = 0;
    int64_t m_val = 0;
    uint8_t m_payload[PayloadSize - 2 * sizeof(int64_t)];
};

template<class TQueue>
class ProducerThread: public Thread
{
public:
    ProducerThread(const int id, TQueue &queue) : m_id(id), m_queue(queue)
    {
    }
    virtual ~ProducerThread()
    {
    }

protected:
    void run() override
    {
        Item item(m_id);
        while (!g_running) {
            // spin lock
        }
        while (g_running) {
            if (m_queue.write(item)) {
                item.newVal();
            }
        }
    }

private:
    int m_id = 0;
    TQueue &m_queue;
};

template<class TQueue>
class ConsumerThread: public Thread
{
public:
    ConsumerThread(TQueue &queue) : m_queue(queue)
    {
    }
    virtual ~ConsumerThread()
    {
    }

protected:
    void run() override
    {
        int64_t countersPerId[NumProducers] = { };
        int64_t valsPerId[NumProducers] = { };
        for (auto &v : valsPerId) v = -1; // first newly filled in element is 0, which must be one larger from stored value ==> start at -1
        int lossCount = 0;
        while (!g_running) {
            // spin lock
        }
        while (g_running) {
            Item item = 0;
            if (m_queue.read(item)) {
                if ((valsPerId[item.channel()] + 1) != item.val()) {
                    ++lossCount;
                    // std::cout << "LOSS #" << item.channel() << " old=" << valsPerId[item.channel()] << " new=" << item.val() << std::endl;
                }
                valsPerId[item.channel()] = item.val();
                ++countersPerId[item.channel()];
            }
        }
        int64_t sum = 0;
        for (int idx = 0; idx < NumProducers; ++idx) {
            std::cout << "#" << idx << ":" << countersPerId[idx] << " ";
            sum += countersPerId[idx];
        }
        std::cout << std::endl << "total:" << sum << "    loss:" << lossCount << std::endl;
    }

private:
    TQueue &m_queue;
};

//#############################################################################

template<class TQueue>
static void testQ()
{
    TQueue queue(QueueCapacity);
    g_running = false;

    // create threads
    ConsumerThread<TQueue> *ct = new ConsumerThread<TQueue>(queue);
    ProducerThread<TQueue> *pt[NumProducers];
    for (int idx = 0; idx < NumProducers; ++idx) {
        pt[idx] = new ProducerThread<TQueue>(idx, queue);
    }

    // wait until all threads have started
    constexpr int NumThreadsOverall = NumProducers + 1;
    for (;;) {
        int numStarted = ct->started();
        for (int idx = 0; idx < NumProducers; ++idx) {
            numStarted += pt[idx]->started();
        }
        if (numStarted == NumThreadsOverall) {
            break;
        }
        usleep(100);
    }

    // run the test
    g_running = true;
    usleep(RunTimeInSeconds * 1000LL * 1000LL);
    g_running = false;

    // cleanup
    delete ct;
    for (auto &t : pt) delete t;
}

int main(int, char**)
{
    std::cout << "StdList/Mutex"              << std::endl; testQ< StdListQueue<Item> >(); std::cout << std::endl;
    std::cout << "Unlocked (not thread safe)" << std::endl; testQ< NoLockQueue <Item> >(); std::cout << std::endl;
    std::cout << "Mutex"                      << std::endl; testQ< MutexQueue  <Item> >(); std::cout << std::endl;
    std::cout << "SpinLock"                   << std::endl; testQ< SpinQueue   <Item> >(); std::cout << std::endl;
    std::cout << "LockFree"                   << std::endl; testQ< LFQueue     <Item> >(); std::cout << std::endl;
    std::cout << "LockFree/StdAtomics"        << std::endl; testQ< LFAQueue    <Item> >(); std::cout << std::endl;
    return 0;
}
