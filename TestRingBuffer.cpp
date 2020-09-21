
#include <iostream>
#include <mutex>
#include "RingBuffer.hpp"

RingBuffer::AlignedAllocator alloc;

int main()
{
    std::mutex mtx;
    std::condition_variable cond;
    std::atomic<int> finished(0);
    auto cosumeList = std::initializer_list<RingBuffer::Int>({1});
    auto ringBuf = RingBuffer::CreateRingBuffer<int, RingBuffer::SingleProducer>(128, &alloc, cosumeList);
    std::thread t1{ [&] {
        auto& pdr = ringBuf.GetProducer();
        for (int i = 0; i < 10000; i++)
        {
            ringBuf.Produce(i);
        }
        finished.fetch_add(1);
        cond.notify_all();
    } };

    std::thread t2{ [&] {
        auto csm = ringBuf.GetConsumer();
        int n = 0;
        while (n < 9999)
        {
            csm.GetNext(n);
        }
        finished.fetch_add(1);
        cond.notify_all();
    } };
    t1.detach();
    t2.detach();

    std::unique_lock<std::mutex> lock{ mtx };
    cond.wait(lock, [&] { return finished >= 2; });

    std::cout << "Test OK!\n"; 
}
