/*
 *
 * Copyright 2026 ZQY
 *
 */

// #include <stdio.h>
// #include <stdbool.h>
// #include <assert.h>

// #include <arpa/inet.h>

// #include <rte_common.h>
// #include <rte_log.h>
// #include <rte_malloc.h>
// #include <rte_memory.h>
// #include <rte_memcpy.h>
// #include <rte_eal.h>
// #include <rte_launch.h>
// #include <rte_atomic.h>
// #include <rte_cycles.h>
// #include <rte_prefetch.h>
// #include <rte_lcore.h>
// #include <rte_per_lcore.h>
// #include <rte_branch_prediction.h>
// #include <rte_interrupts.h>
// #include <rte_random.h>
// #include <rte_debug.h>
// #include <rte_ether.h>
// #include <rte_ethdev.h>
// #include <rte_mempool.h>
// #include <rte_mbuf.h>
// #include <rte_bus_pci.h>

// /* workaround to avoid conflicts between dpdk and lwip definitions */
// #undef IP_DF
// #undef IP_MF
// #undef IP_RF
// #undef IP_OFFMASK

// #include <lwip/opt.h>
// #include <lwip/init.h>
// #include <lwip/pbuf.h>
// #include <lwip/netif.h>
// #include <lwip/etharp.h>
// #include <lwip/tcpip.h>
// #include <lwip/tcp.h>
// #include <lwip/timeouts.h>
// #include <lwip/prot/tcp.h>
// #include <lwip/logging.h>
// #include <lwip/thread_framework.h>

// #include <netif/ethernet.h>

// int main()
// {

//     printf("hello world\n");

//     thread_framework_init(1, 2);


//     return 0;
// }

#include <rte_ring.h>
#include <rte_malloc.h>
#include <rte_eal.h>
#include <stdio.h>
#include <stdlib.h>
#include <pthread.h>
#include <unistd.h>

#define RING_SIZE 1024          /* 必须是2的幂次 */
#define NUM_PRODUCERS 4
#define NUM_ITERATIONS 1000

/* 存储在队列中的对象示例 */
struct data_obj {
    int id;
    char payload[64];
};

/* 全局队列指针 */
static struct rte_ring *g_ring = NULL;

/* 生产者线程函数 */
static void *producer_thread(void *arg) {
    int thread_id = *(int *)arg;
    unsigned i;

    for (i = 0; i < NUM_ITERATIONS; i++) {
        /* 分配对象（实际场景可能使用内存池） */
        struct data_obj *obj = rte_malloc("obj", sizeof(struct data_obj), 0);
        if (!obj) {
            fprintf(stderr, "Producer %d: rte_malloc failed\n", thread_id);
            break;
        }

        obj->id = thread_id * 10000 + i;
        snprintf(obj->payload, sizeof(obj->payload), "from prod %d", thread_id);

        /* 多生产者入队：rte_ring_enqueue 内部已保证多线程安全 */
        int ret;
        do {
            ret = rte_ring_enqueue(g_ring, obj);
            if (ret != 0) {
                /* 队列满，等待后重试（此处简单使用usleep） */
                usleep(10);
            }
        } while (ret != 0);
    }
    return NULL;
}

/* 消费者线程函数（仅单线程调用） */
static void *consumer_thread(void *arg) {
    (void)arg;  /* 未使用 */
    void *obj_ptr;

    while (1) {
        /* 单消费者出队：因创建时指定了 RING_F_SC_DEQ，无需锁 */
        if (rte_ring_dequeue(g_ring, &obj_ptr) == 0) {
            struct data_obj *obj = (struct data_obj *)obj_ptr;
            printf("Consumer got obj id=%d, payload=%s\n", obj->id, obj->payload);
            rte_free(obj);
        } else {
            /* 队列空，可加入适当延迟或等待 */
            usleep(10);
        }
    }
    return NULL;
}

int main(int argc, char *argv[]) {
    int ret;
    pthread_t producers[NUM_PRODUCERS];
    pthread_t consumer;
    int thread_ids[NUM_PRODUCERS];
    unsigned i;

    printf("main: 1\n");

    /* 1. 初始化DPDK环境抽象层 */
    ret = rte_eal_init(argc, argv);
    if (ret < 0) {
        rte_exit(EXIT_FAILURE, "Cannot init EAL\n");
    }

    printf("main: 2\n");

    /* 2. 创建多生产者单消费者无锁队列 */
    g_ring = rte_ring_create("MPSC_RING", RING_SIZE,
                              rte_socket_id(),   /* 当前NUMA节点 */
                              RING_F_SC_DEQ);    /* 单消费者出队标志 */
    if (g_ring == NULL) {
        rte_exit(EXIT_FAILURE, "Cannot create ring\n");
    }

    /* 3. 启动消费者线程（单线程） */
    if (pthread_create(&consumer, NULL, consumer_thread, NULL) != 0) {
        perror("pthread_create consumer");
        exit(1);
    }

    /* 4. 启动多个生产者线程 */
    for (i = 0; i < NUM_PRODUCERS; i++) {
        thread_ids[i] = i;
        if (pthread_create(&producers[i], NULL, producer_thread, &thread_ids[i]) != 0) {
            perror("pthread_create producer");
            exit(1);
        }
    }

    /* 5. 等待生产者线程结束（示例中消费者会一直运行，可按需终止） */
    for (i = 0; i < NUM_PRODUCERS; i++) {
        pthread_join(producers[i], NULL);
    }

    /* 注意：实际应用中需有机制通知消费者退出（如设置全局标志） */
    /* 这里简单等待几秒后强制退出（仅供演示） */
    sleep(5);
    pthread_cancel(consumer);   /* 不推荐，仅作演示 */
    pthread_join(consumer, NULL);

    /* 6. 清理资源 */
    rte_ring_free(g_ring);
    rte_eal_cleanup();
    return 0;
}
