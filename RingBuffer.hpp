/*
 * A simple RingBuffer implementation
 * Inspired by a Java library: Disruptor (https://github.com/LMAX-Exchange/disruptor)
 * (C)daipech@gmail.com
 */

#pragma once

#ifndef _MRINGBUFFER_HPP_
#define _MRINGBUFFER_HPP_

#include <thread>
#include <atomic>
#include <cassert>

#if defined(_MSC_VER)
# define ALIGNED_ALLOC   _aligned_malloc
# define ALIGNED_FREE    _aligned_free
#else
# define ALIGNED_ALLOC   aligned_alloc
# define ALIGNED_FREE    aligned_free
#endif

// algined new operator

template <class A>
void* operator new (size_t size, A* a, int align)
{
    return a->Allocate(size, align);
}

template <class A>
void*  operator new[](size_t size, A* a, int align)
{
    return a->Allocate(size, align);
}

template <class A>
void Free(A* a, void* p)
{
    a->Deallocate(p);
}

template <class A, class T>
void Delete(A* a, T* p)
{
    if constexpr (!std::is_void_v<T>)
    {
        p->~T();
    }
    Free(a, p);
}

// only for new operator failed
template <class A>
void operator delete (void* mem, A* a, int align)
{
    Free(a, mem);
}

template <class A>
void  operator delete[](void* mem, A* a, int align)
{
    Free(a, mem);
}

namespace RingBuffer
{

    typedef signed   char      Char;     // 8 bit signed
    typedef signed   short     Short;    // 16bit signed
    typedef signed   int       Int;      // 32bit signed
    typedef signed   long long Long;     // 64bit signed

    typedef unsigned char      Byte;     // 8 bit unsigned
    typedef unsigned short     Word;     // 16bit unsigned
    typedef unsigned int       DWord;    // 32bit unsigned
    typedef unsigned long long QWord;    // 64bit unsigned

    typedef float  Float;                // single precision
    typedef double Double;               // double precision

    // is a number of 2^n?
    constexpr bool is2n(DWord n)
    {
        DWord tmp = n - 1;
        return ((tmp & n) == 0);
    }

    // Some useful template
    template <typename T>
    T Min(const T& t1, const T& t2) { return t1 < t2 ? t1 : t2; }

    template <typename T>
    T Max(const T& t1, const T& t2) { return t1 > t2 ? t1 : t2; }

    template <typename T>
    T Abs(const T& t1) { return t1 > 0 ? t1 : -t1; }

    template <typename T>
    T Clamp(const T& t0, const T& t1, const T& t2)
    {
        return t0 < t1 ? t1 : t0 > t2 ? t2 : t0;
    }

    static constexpr Int DEFAULT_ALIGN = sizeof(void*);

    class AlignedAllocator
    {
    public:
        inline void* Allocate(size_t size, int align)
        {
            return ALIGNED_ALLOC(size, align);
        }
        inline void Deallocate(void* ptr)
        {
            ALIGNED_FREE(ptr);
        }
    };

    // Memory Aligned Value
    template <class T, std::size_t N>
    struct AlignedValue
    {
        alignas(N) T value;
        inline AlignedValue() = default;
        //inline AlignedValue(const T& val) : value(val) {}
        //inline AlignedValue(const T&& val) : value(val) {}
        inline T& operator -> () { return value; }
    };

    // Most machine has 64byte chacheline size
    static constexpr Int CACHELINE_SIZE = 64;

    // Cacheline Aligned Value, used to avoid "false data sharing"
    template <class T>
    using CachelineValue = AlignedValue<T, CACHELINE_SIZE>;

    // volatile and thread safe array access
    template <class T>
    inline T GetArrayVolatile(T* array, int offset)
    {
        volatile T* ptr = (volatile T*)(&array[offset]);
        std::atomic_thread_fence(std::memory_order::memory_order_acquire);
        return *ptr;
    }

    template <class T>
    inline void SetArrayVolatile(T* array, int offset, const T& value)
    {
        volatile T* ptr = (volatile T*)(&array[offset]);
        std::atomic_thread_fence(std::memory_order::memory_order_release);
        *ptr = value;
    }

    // element container
    template <class T, class A>
    class Container
    {
    public:
        Container(Int size, A* alloc)
        {
            assert(is2n(size));
            this->bufferSize = size;
            this->alloc = alloc;
            this->entries = new (alloc, DEFAULT_ALIGN) T[size];
            indexMask = size - 1;
        }
        Container(Container&& other)
        {
            this->bufferSize = other.bufferSize;
            this->alloc = other.alloc;
            this->entries = other.entries;
            this->indexMask = other.indexMask;
            other.bufferSize = 0;
            other.entries = nullptr;
            other.indexMask = -1;
        }
        virtual ~Container()
        {
            if (this->entries)
            {
                Delete(alloc, (T*)this->entries);
                this->entries = nullptr;
            }
            indexMask = -1;
            bufferSize = 0;
        }
        inline const T Get(Long idx) const { return entries[(idx & indexMask)]; }
        //inline T& Get(Long idx) { return entries[(idx & indexMask)]; }
        inline void Set(Long idx, const T& t) { entries[(idx & indexMask)] = t; }
        inline A* GetAllocator() { return alloc; }
        inline const T& operator [] (Long idx) const { return Get(idx); }
        inline T& operator [] (Long idx) { return Get(idx); }
    private:
        QWord indexMask;
        volatile T* entries;
        Int bufferSize;
        A* alloc;
    };

    // Cacheline aligned and shared safe 64bit index
    typedef CachelineValue<std::atomic<Long>> SafeIndex;

    // Abstract Producer
    struct Producer
    {
        Int bufferSize;
        Int consumerCount;
        SafeIndex cursor;
        SafeIndex* consumerList = nullptr;

        inline Producer(Int size) : bufferSize(size), cursor(), consumerCount(0)
        {
            this->cursor.value.store(-1);
        }
        inline Producer(Producer&& other) : cursor()
        {
            this->cursor.value.store(-1);
            this->bufferSize = other.bufferSize;
            this->consumerCount = other.consumerCount;
            this->consumerList = other.consumerList;
        }
        virtual ~Producer() {}
        inline void SetConsumers(SafeIndex* list, Int count)
        {
            consumerCount = count;
            consumerList = list;
        }
        inline Long GetCursor() { return cursor.value.load(); }
        inline Long GetMinimumIndex(Long minimum)
        {
            for (int i = 0; i < consumerCount; i++)
            {
                Long val = consumerList[i].value.load();
                if (val >= 0)
                {
                    minimum = Min(val, minimum);
                }
            }
            return minimum;
        }
        inline Long GetMinimumIndex()
        {
            return GetMinimumIndex(cursor.value.load());
        }
        virtual Long GetPublishedIndex(Long cached) = 0;
    };

    // Cached next value for single producer
    struct SimpleNext
    {
        Long next;
        Long cached;
    };

    // The RingBuffer have only one producer
    class SingleProducer : public Producer
    {
    public:
        template <class A>
        SingleProducer(Int size, A* alloc) : Producer(size)
        {
            nextValue.value.next = -1;
            nextValue.value.cached = -1;
        }
        SingleProducer(SingleProducer&& other) : Producer(std::move(other)) {}
        virtual ~SingleProducer() {}
        bool HasAvailableCapacity(int size, bool store = false)
        {
            Long next = GetNextValue();
            Long cached = GetCachedValue();
            Long wrap = next + size - this->bufferSize;
            if (wrap > cached || cached > next)
            {
                if (store) { this->cursor.value.store(next); }
                Long minSeq = GetMinimumIndex(next);
                SetCachedValue(minSeq);
                if (wrap > minSeq) { return false; }
            }
            return true;
        }
        Long Next(Int count = 1)
        {
            assert(count > 0);
            Long next = GetNextValue();
            Long cached = GetCachedValue();
            Long nextSeq = next + count;
            Long wrap = nextSeq - this->bufferSize;
            if (wrap > cached || cached > next)
            {
                this->cursor.value.store(next);
                Long minSeq;
                while (wrap > (minSeq = this->GetMinimumIndex(next)))
                {
                    /* TODO: sleep */
                }
                SetCachedValue(minSeq);
            }
            SetNextValue(nextSeq);
            return nextSeq;
        }
        Long TryNext(Int count = 1)
        {
            assert(count > 0);
            if (HasAvailableCapacity(count, true))
            {
                Long next = GetNextValue() + count;
                SetNextValue(next);
                return next;
            }
            return -1;
        }
        Long RemainingCapacity()
        {
            Long next = GetNextValue();
            Long consumed = GetMinimumIndex(next);
            return this->bufferSize - (next - consumed);
        }
        void Claim(Long idx) { SetNextValue(idx); }
        void Publish(Long idx) { this->cursor.value.store(idx); }
        void Publish(Long first, Long last) { Publish(last); }
        bool IsAvailable(Long idx) { return idx <= cursor.value.load(); }
        Long GetPublishedIndex(Long cached) { return GetCursor(); }
    protected:
        inline Long GetNextValue() { return nextValue.value.next; }
        inline Long GetCachedValue() { return nextValue.value.cached; }
        inline void SetNextValue(Long val) { nextValue.value.next = val; }
        inline void SetCachedValue(Long val) { nextValue.value.cached = val; }
    protected:
        CachelineValue<SimpleNext> nextValue;
    };

    // The RingBuffer have many Producer
    template <class A>
    class MultiProducer : public Producer
    {
    public:
        MultiProducer(Int size, A* alloc) : Producer(size), alloc(alloc), cachedConsume(-1)
        {
            indexMask = size - 1;
            indexShift = std::log2(size);
            publishList = new (alloc, CACHELINE_SIZE) Int[size];
            for (int i = 0; i < size; i++)
            {
                SetArrayVolatile<Int>(publishList, i, -1);
            }
        }
        MultiProducer(MultiProducer&& other) : Producer(std::move(other)), cachedConsume(-1)
        {
            this->alloc = other.alloc;
            this->indexMask = other.indexMask;
            this->indexShift = other.indexShift;
            this->publishList = other.publishList;
            other.publishList = nullptr;
        }
        virtual ~MultiProducer()
        {
            if (publishList)
            {
                Delete(alloc, publishList);
            }
        }
        bool HasAvailableCapacity(int size)
        {
            Long cur = GetCursor();
            Long wrap = cur + size - bufferSize;
            Long cached = cachedConsume.value.load(std::memory_order::memory_order_consume);

            if (wrap > cached || cached > cur)
            {
                Long csm = GetMinimumIndex();
                cachedConsume.value.store(csm, std::memory_order::memory_order_acquire);

                return wrap > csm;
            }
            return true;
        }
        void Claim(Long idx) { this->cursor.value.store(idx, std::memory_order::memory_order_acquire); }
        Long Next(Int count = 1)
        {
            assert(count > 0);
            Long cur, next;
            do {
                cur = GetCursor();
                next = cur + count;
                Long wrap = next - bufferSize;
                Long cached = cachedConsume.value.load(std::memory_order::memory_order_consume);
                if (wrap < cached || cached > cur)
                {
                    Long csm = GetMinimumIndex();
                    if (wrap > csm)
                    {
                        //TODO: sleep?
                        continue;
                    }

                    cachedConsume.value.store(csm, std::memory_order::memory_order_acquire);
                }
                else if (this->cursor.value.compare_exchange_strong(cur, next))
                {
                    break;
                }
            } while (true);

            return next;
        }
        Long TryNext(Int count = 1)
        {
            assert(count > 0);
            Long cur, next;
            cur = GetCursor();
            do {
                next = cur + count;
                if (!HasAvailableCapacity(count))
                {
                    return -1;
                }
            } while (this->cursor.value.compare_exchange_strong(cur, next));
            return next;
        }
        Long RemainingCapacity()
        {
            Long csm = GetMinimumIndex();
            Long prd = GetCursor();
            return bufferSize - (prd - csm);
        }
        inline bool IsAvailable(Long idx)
        {
            return GetArrayVolatile(publishList, (int)(idx & indexMask)) == (int)(idx >> indexShift);
        }
        inline void SetAvailable(Long idx)
        {
            SetArrayVolatile<Int>(publishList, (int)(idx & indexMask), (Int)(idx >> indexShift));
        }
        void Publish(Long idx) { SetAvailable(idx); }
        void Publish(Long first, Long last)
        {
            for (Long l = first; l <= last; l++)
            {
                SetAvailable(l);
            }
        }
        Long GetPublishedIndex(Long cached)
        {
            Long cur = GetCursor();
            for (Long l = cached; l <= cur; l++)
            {
                if (!IsAvailable(l))
                {
                    return l - 1;
                }
            }
            return cur;
        }
    protected:
        A* alloc;
        volatile Int* publishList;
        DWord indexMask;
        DWord indexShift;
        SafeIndex cachedConsume;
    };

    // a group of consumer shared one cursor
    struct ConsumerGroup
    {
        SafeIndex cursor;
        Int startPos;
        Int size;
    };

    // Consumer
    template <class T, class A>
    class Consumer
    {
    public:
        Consumer() = default;
        Consumer(Container<T, A>* buf, Producer* prdcer, SafeIndex* groupIdx, SafeIndex* consumeIdx) :
            buffer(buf), producer(prdcer), groupIndex(groupIdx), consumeIndex(consumeIdx), cachedAvaliable(-1) {}
        Consumer(Consumer&& other) : buffer(other.buffer), producer(other.producer),
            groupIndex(other.groupIndex), consumeIndex(other.consumeIndex), cachedAvaliable(-1) {}
        Long GetAvailable()
        {
            cachedAvaliable = producer->GetPublishedIndex(cachedAvaliable);
            return cachedAvaliable;
        }
        Int GetAvailableCount()
        {
            Long prev = groupIndex->value.load(std::memory_order::memory_order_consume);
            return (Int)GetAvailable() - prev + 1;
        }
        Long Next(Int count = 1)
        {
            Long next;
            Long prev;
            prev = groupIndex->value.load(std::memory_order::memory_order_consume);
            do {
                next = prev + count;
                if (next > cachedAvaliable)
                {
                    if (next > GetAvailable())
                    {
                        //TODO: wait and sleep
                        continue;
                    }
                }
            } while (groupIndex->value.compare_exchange_strong(prev, next));
            return next;
        };
        Long TryNext(Int count = 1)
        {
            Long next;
            Long prev;
            prev = groupIndex->value.load(std::memory_order::memory_order_consume);
            while (true)
            {
                next = prev + count;
                if (next > cachedAvaliable)
                {
                    if (next > GetAvailable())
                    {
                        //TODO: wait and sleep
                        continue;
                    }
                }

                if (groupIndex->value.compare_exchange_strong(prev, next))
                {
                    break;
                }
            };
            return next;
        }
        void Consume(Long idx)
        {
            consumeIndex->value.store(idx, std::memory_order::memory_order_seq_cst);
        }
        void GetNext(T& obj)
        {
            Long idx = Next();
            obj = buffer->Get(idx);
            Consume(idx);
        }
        bool TryGetNext(T& obj)
        {
            Long idx = TryNext();
            if (idx >= 0)
            {
                obj = buffer->Get(idx);
                Consume(idx);
                return true;
            }
            return false;
        }
        Container<T, A>* GetBuffer() { return buffer; }
    private:
        Long cachedAvaliable;
        Container<T, A>* buffer;
        Producer* producer;
        SafeIndex* groupIndex;
        SafeIndex* consumeIndex;
    };

    // RingBuffer
    template <class T, class P, class A>
    class Buffer
    {
    public:
        using ConsumerType = Consumer<T, A>;
        using ProducerType = P;
    public:
        Buffer(Int size, A* alloc, std::initializer_list<Int>& groupList) : buffer(size, alloc), producer(size, alloc)
        {
            groupNum = (Int)groupList.size();
            groups = new (alloc, CACHELINE_SIZE) ConsumerGroup[groupNum];
            int i = 0;
            int num = 0;
            for (Int n : groupList)
            {
                groups[i].cursor.value.store(-1);
                groups[i].size = n;
                groups[i].startPos = num;
                i++;
                num += n;
            }
            consumerNum = num;
            consumeList = new (alloc, CACHELINE_SIZE) SafeIndex[num];
        }
        Buffer(Buffer&& other) : buffer(std::move(other.buffer)), producer(std::move(other.producer))
        {
            this->consumerNum = other.consumerNum;
            this->groupNum = other.groupNum;
            this->consumeList = other.consumeList;
            this->groups = other.groups;
            other.consumerNum = 0;
            other.groupNum = 0;
            other.consumeList = nullptr;
            other.groups = nullptr;
        }
        virtual ~Buffer()
        {
            if (groups && consumeList)
            {
                Delete(buffer.GetAllocator(), groups);
                Delete(buffer.GetAllocator(), consumeList);
            }
        }
        inline P& GetProducer() { return producer; }
        inline Container<T, A>& GetContainer() { return buffer; }
        inline Int GetGroupCount() { return groupNum; }
        inline ConsumerGroup& GetGroup(Int groupId)
        {
            assert(groupId < groupNum);
            return groups[groupId];
        }
        inline void Produce(const T& t)
        {
            Long idx = producer.Next();
            buffer.Set(idx, t);
            producer.Publish(idx);
        }
        inline bool TryProduce(const T& t)
        {
            Long idx = producer.TryNext();
            if (idx >= 0)
            {
                buffer.Set(idx, t);
                producer.Publish(idx);
                return true;
            }
            return false;
        }
        inline Consumer<T, A> GetConsumer(Int groupId = 0, Int index = 0)
        {
            assert(groupId >= 0 && groupId < groupNum);
            auto& group = GetGroup(groupId);
            assert(index >= 0 && index < group.size);
            return { &buffer, &producer, &group.cursor, &consumeList[group.startPos + index] };
        }
    private:
        Container<T, A> buffer;
        P producer;
        Int consumerNum;
        Int groupNum;
        SafeIndex* consumeList;
        ConsumerGroup* groups;
    };

    template <class T, class P, class A>
    Buffer<T, P, A> CreateRingBuffer(Int size, A* alloc, std::initializer_list<Int>& groupList)
    {
        return {size, alloc, groupList};
    }

} // end namespace RingBuffer

#endif // !_MRINGBUFFER_HPP_

