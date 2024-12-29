/*
 *
 * Copyright 2022 Kenichi Yasukata
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

#include <stdio.h>
#include <stdbool.h>
#include <assert.h>

#include <arpa/inet.h>

#include <rte_common.h>
#include <rte_log.h>
#include <rte_malloc.h>
#include <rte_memory.h>
#include <rte_memcpy.h>
#include <rte_eal.h>
#include <rte_launch.h>
#include <rte_atomic.h>
#include <rte_cycles.h>
#include <rte_prefetch.h>
#include <rte_lcore.h>
#include <rte_per_lcore.h>
#include <rte_branch_prediction.h>
#include <rte_interrupts.h>
#include <rte_random.h>
#include <rte_debug.h>
#include <rte_ether.h>
#include <rte_ethdev.h>
#include <rte_mempool.h>
#include <rte_mbuf.h>
#include <rte_bus_pci.h>

/* workaround to avoid conflicts between dpdk and lwip definitions */
#undef IP_DF
#undef IP_MF
#undef IP_RF
#undef IP_OFFMASK

#include <lwip/opt.h>
#include <lwip/init.h>
#include <lwip/pbuf.h>
#include <lwip/netif.h>
#include <lwip/etharp.h>
#include <lwip/tcpip.h>
#include <lwip/tcp.h>
#include <lwip/timeouts.h>
#include <lwip/prot/tcp.h>
#include <lwip/logging.h>
#include <lwip/thread_framework.h>

#include <netif/ethernet.h>

// #define MAX_PKT_BURST (32)
#define MAX_PKT_BURST (256)
// #define NUM_SLOT (256)
#define NUM_SLOT (512)



#define MEMPOOL_CACHE_SIZE (256)

#define PACKET_BUF_SIZE (1518)

static struct rte_mempool *pktmbuf_pool = NULL;

// extern struct rte_mempool *pktmbuf_pool_tcp_tx = NULL;


static _Thread_local int tx_idx = 0;
static _Thread_local struct rte_mbuf *tx_mbufs[MAX_PKT_BURST] = { 0 };

// _Thread_local volatile int thread_tx_queue_id = 0; // default 0, tcp thread set it to sepcific id;
// extern _Thread_local volatile int thread_tx_queue_id; // default 0, tcp thread set it to sepcific id;



// static char *httpbuf;
// static size_t httpdatalen;

/* static */ void tx_flush(void)
{
	if  (tx_idx > 0)
	    LOG_DEBUG("tx_flush: tx_idx = %d, thread_tx_queue_id: %d\n", tx_idx, thread_tx_queue_id);
	
	int try_num = 0;

	assert(thread_tx_queue_id >= 0 && thread_tx_queue_id < 1 + g_tcp_thread_num);
	
	int xmit = tx_idx, xmitted = 0;
	while (xmitted != xmit)
	{
		int ret = rte_eth_tx_burst(0 /* port id */, thread_tx_queue_id /* queue id */, &tx_mbufs[xmitted], xmit - xmitted);
		xmitted += ret;

		if (++try_num > 10)
		{
			LOG_INFO("tx_flush, thread_tx_queue_id: %d, ret :%d, xmit: %d, xmitted: %d\n",
					 thread_tx_queue_id, ret, xmit, xmitted);
		}
	}

	tx_idx = 0;
}

static err_t low_level_output(struct netif *netif __attribute__((unused)), struct pbuf *p)
{
	LOG_DEBUG("low_level_output: 1, p->tot_len = %d\n", p->tot_len);

	char buf[PACKET_BUF_SIZE];
	void *bufptr, *largebuf = NULL;
	if (sizeof(buf) < p->tot_len) {
		largebuf = (char *) malloc(p->tot_len);
		assert(largebuf);
		bufptr = largebuf;
	} else
		bufptr = buf;

	pbuf_copy_partial(p, bufptr, p->tot_len, 0);

	assert((tx_mbufs[tx_idx] = rte_pktmbuf_alloc(pktmbuf_pool)) != NULL);
	assert(p->tot_len <= RTE_MBUF_DEFAULT_BUF_SIZE);
	rte_memcpy(rte_pktmbuf_mtod(tx_mbufs[tx_idx], void *), bufptr, p->tot_len);
	rte_pktmbuf_pkt_len(tx_mbufs[tx_idx]) = rte_pktmbuf_data_len(tx_mbufs[tx_idx]) = p->tot_len;
	if (++tx_idx == MAX_PKT_BURST)
		tx_flush();

	if (largebuf)
		free(largebuf);

	LOG_DEBUG("low_level_output: 2, end\n");
	return ERR_OK;
}

static unsigned long io_stat[3] = { 0 };

struct http_response {
	int state;
	long content_tot_len;
	long content_recvd;
	long cur;
	char buf[1UL << 16];
};

// temp buf, not for multi_thread
_Thread_local char tcp_recv_temp_buf[2048];

int tcp_recv_copy_data(struct pbuf* p, char* buf, int max_len)
{
	int copy_len = p->tot_len < max_len ? p->tot_len : max_len;

    pbuf_copy_partial(p, buf, copy_len, 0);

	return copy_len;
}

uint64_t recv_pkt_cnt[TCP_THREAD_MAX_NUM];
uint64_t recv_pkt_byte_cnt[TCP_THREAD_MAX_NUM];

static err_t tcp_recv_handler(void *arg, struct tcp_pcb *tpcb,
			      struct pbuf *p, err_t err)
{
	// LOG_DEBUG("tcp_recv_handler: 1, p->tot_len = %d\n", p->tot_len);
    LOG_DEBUG("tcp_recv_handler: 1\n");


	if (err != ERR_OK)
		return err;
	if (!p) {
		tcp_close(tpcb);
		return ERR_OK;
	}
	io_stat[1] += p->tot_len;
	LOG_DEBUG("tcp_recv_handler: 2, p->tot_len: %d\n", p->tot_len);

	if (!arg) { /* server mode */
		// char buf[4] = { 0 };
		int copy_len = (p->tot_len < 2048 ? p->tot_len : 2048);
		pbuf_copy_partial(p, tcp_recv_temp_buf, copy_len, 0);

		recv_pkt_cnt[thread_tx_queue_id - 1]++;
		recv_pkt_byte_cnt[thread_tx_queue_id - 1] += copy_len;

		// if (!strncmp(buf, "GET", 3)) {
		// 	io_stat[0]++;
		// 	io_stat[2] += httpdatalen;
		// 	assert(tcp_sndbuf(tpcb) >= httpdatalen);
		// 	assert(tcp_write(tpcb, httpbuf, httpdatalen, TCP_WRITE_FLAG_COPY) == ERR_OK);
		// 	assert(tcp_output(tpcb) == ERR_OK);
		// }
		assert(tcp_sndbuf(tpcb) >= copy_len);
		assert(tcp_write(tpcb, tcp_recv_temp_buf, copy_len, TCP_WRITE_FLAG_COPY) == ERR_OK);
		assert(tcp_output(tpcb) == ERR_OK);

	} else { /* client mode */
		// struct http_response *r = (struct http_response *) arg;
		// assert(p->tot_len < (sizeof(r->buf) - r->cur));
		// pbuf_copy_partial(p, &r->buf[r->cur], p->tot_len, 0);
	// 	r->cur += p->tot_len;
	// 	switch (r->state) {
	// 	case 0:
	// 		{
	// 			long i;
	// 			for (i = 0; i < r->cur && r->state == 0; i++) {
	// 				if (r->buf[i] == 'C') {
	// 					if (r->cur - i > 15) {
	// 						if (!memcmp(&r->buf[i], "Content-Length:", 15)) {
	// 							long j;
	// 							for (j = 0; j < (r->cur - i - 15); j++) {
	// 								if (r->buf[i + 15 + j] == '\r') {
	// 									r->buf[i + 15 + j] = '\0';
	// 									assert(sscanf(&r->buf[i], "Content-Length: %ld", &r->content_tot_len) == 1);
	// 									r->buf[i + 15 + j] = '\r';
	// 									r->state = 1;
	// 									break;
	// 								}
	// 							}
	// 						}
	// 					}
	// 				}
	// 			}
	// 		}
	// 		/* fall through */
	// 	case 1:
	// 		{
	// 			long i;
	// 			for (i = 0; i <= r->cur - 4 && r->state == 1; i++) {
	// 				if (r->buf[i + 0] == '\r' && r->buf[i + 1] == '\n' && r->buf[i + 2] == '\r' && r->buf[i + 3] == '\n') {
	// 					r->cur -= i + 4;
	// 					r->content_recvd = 0;
	// 					r->state = 2;
	// 					break;
	// 				}
	// 			}
	// 		}
	// 		/* fall through */
	// 	case 2:
	// 		r->content_recvd += r->cur;
	// 		if (r->content_recvd == r->content_tot_len) {
	// 			io_stat[0]++;
	// 			io_stat[2] += 42;
	// 			assert(tcp_sndbuf(tpcb) >= 42);
	// 			assert(tcp_write(tpcb, "GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n", 42, TCP_WRITE_FLAG_COPY) == ERR_OK);
	// 			assert(tcp_output(tpcb) == ERR_OK);
	// 			r->state = 0;
	// 		}
	// 		r->cur = 0;
	// 		break;
	// 	default:
	// 		assert(0);
	// 		break;
	// 	}

		int copy_len = (p->tot_len < 2048 ? p->tot_len : 2048);
		pbuf_copy_partial(p, tcp_recv_temp_buf, copy_len, 0);

		recv_pkt_cnt[thread_tx_queue_id - 1]++;
		recv_pkt_byte_cnt[thread_tx_queue_id - 1] += copy_len;

        assert(tcp_sndbuf(tpcb) >= copy_len);
		assert(tcp_write(tpcb, tcp_recv_temp_buf, copy_len, TCP_WRITE_FLAG_COPY) == ERR_OK);
		assert(tcp_output(tpcb) == ERR_OK);

	}
	tcp_recved(tpcb, p->tot_len);
	pbuf_free(p);


	LOG_DEBUG("tcp_recv_handler: 3, end\n");
	return ERR_OK;
}

static void tcp_destroy_handeler(unsigned char id __attribute__((unused)), void *data)
{
	if (data)
		free(data);
}

static const struct tcp_ext_arg_callbacks tcp_ext_arg_cbs =  {
	.destroy = tcp_destroy_handeler,
};

static err_t accept_handler(void *arg __attribute__((unused)), struct tcp_pcb *tpcb, err_t err)
{
	LOG_DEBUG("accept_handler: 1, enter\n");
	if (err != ERR_OK)
		return err;

	tcp_recv(tpcb, tcp_recv_handler);
	tcp_setprio(tpcb, TCP_PRIO_MAX);

	tcp_ext_arg_set_callbacks(tpcb, 0, &tcp_ext_arg_cbs);
	tcp_ext_arg_set(tpcb, 0, NULL);

	tcp_nagle_disable(tpcb);

	tpcb->so_options |= SOF_KEEPALIVE;
	tpcb->keep_intvl = (60 * 1000);
	tpcb->keep_idle = (60 * 1000);
	tpcb->keep_cnt = 1;

	LOG_DEBUG("accept_handler: 2, end\n");
	return err;
}

static err_t connected_handler(void *arg, struct tcp_pcb *tpcb, err_t err)
{
	LOG_INFO("connected_handler: 1, begin -----------------------------\n");
	// while (1) sleep(1);

	if (err != ERR_OK)
		return err;
	if ((err = accept_handler(arg, tpcb, err)) != ERR_OK)
		return err;

	// io_stat[2] += 42;
	int copy_len = 64;
	assert(tcp_sndbuf(tpcb) >= copy_len);
	// assert(tcp_write(tpcb, "GET / HTTP/1.0\r\nConnection: Keep-Alive\r\n\r\n", 42, TCP_WRITE_FLAG_COPY) == ERR_OK);
	assert(tcp_write(tpcb, tcp_recv_temp_buf, copy_len, TCP_WRITE_FLAG_COPY) == ERR_OK);
	assert(tcp_output(tpcb) == ERR_OK);

	return ERR_OK;
}

static err_t if_init(struct netif *netif)
{
	{
		struct rte_ether_addr ports_eth_addr;
		assert(rte_eth_macaddr_get(0 /* port id */, &ports_eth_addr) >= 0);
		for (int i = 0; i < 6; i++)
			netif->hwaddr[i] = ports_eth_addr.addr_bytes[i];
	}
	{
		uint16_t _mtu;
		assert(rte_eth_dev_get_mtu(0 /* port id */, &_mtu) >= 0);
		assert(_mtu <= PACKET_BUF_SIZE);
		netif->mtu = _mtu;
	}
	netif->output = etharp_output;
	netif->linkoutput = low_level_output;
	netif->hwaddr_len = 6;
	netif->flags = NETIF_FLAG_BROADCAST | NETIF_FLAG_ETHARP | NETIF_FLAG_IGMP;
	return ERR_OK;
}


unsigned short netif_poll_once(struct netif* _netif_p, int queue_id)
{
	// LOG_DEBUG("main: 7.1\n");
	struct rte_mbuf *rx_mbufs[MAX_PKT_BURST];
	unsigned short i, nb_rx = rte_eth_rx_burst(0 /* port id */, queue_id /* queue id */, rx_mbufs, MAX_PKT_BURST);

	// LOG_DEBUG("main: 7.2\n");
	for (i = 0; i < nb_rx; i++)
	{
		LOG_DEBUG("main: 7.3\n");
		{
			LOG_DEBUG("main: 7.4\n");
			struct pbuf *p;
			// assert((p = pbuf_alloc(PBUF_RAW, rte_pktmbuf_pkt_len(rx_mbufs[i]), PBUF_POOL)) != NULL);
			// assert((p = pbuf_alloc_from_rte_malloc(rte_pktmbuf_pkt_len(rx_mbufs[i]))) != NULL);
			// assert((p = pbuf_alloc(PBUF_RAW, rte_pktmbuf_pkt_len(rx_mbufs[i]), PBUF_RTE_MALLOC)) != NULL);
			assert((p = pbuf_alloc(PBUF_RAW, rte_pktmbuf_pkt_len(rx_mbufs[i]), PBUF_RTE_MBUF_RX)) != NULL);


			LOG_DEBUG("main: 7.5\n");
			pbuf_take(p, rte_pktmbuf_mtod(rx_mbufs[i], void *), rte_pktmbuf_pkt_len(rx_mbufs[i]));
			rte_pktmbuf_free(rx_mbufs[i]);

			LOG_DEBUG("main: 7.6, p->payload: %lx, rte data: %lx\n",
					  (long)p->payload, (long)rte_pktmbuf_mtod(rx_mbufs[i], void *));
			p->len = p->tot_len = rte_pktmbuf_pkt_len(rx_mbufs[i]);
			assert(_netif_p->input(p, _netif_p) == ERR_OK);

			LOG_DEBUG("main: 7.7\n");
		}
		// rte_pktmbuf_free(rx_mbufs[i]);
	}

	return nb_rx;
}

int max_epoll_wait_timeout_ms = 0;

void netif_rx_test_sleep(unsigned short nb_rx, uint16_t queue_id)
{
	LOG_DEBUG("main: 7.11, nb_rx: %d, max_epoll_wait_timeout_ms: %d, queue_id: %d\n",
			  nb_rx, max_epoll_wait_timeout_ms, queue_id);

	if (!nb_rx && max_epoll_wait_timeout_ms)
	{

		LOG_DEBUG("main: 7.12\n");
		assert(!rte_eth_dev_rx_intr_enable(0 /* port id */, queue_id /* queue id */));
		{
			struct rte_epoll_event ev;
			(void)rte_epoll_wait(RTE_EPOLL_PER_THREAD, &ev, 1, max_epoll_wait_timeout_ms < 0 ? 100 : (max_epoll_wait_timeout_ms > 100 ? 100 : max_epoll_wait_timeout_ms));
		}

		LOG_DEBUG("main: 7.13\n");
		rte_eth_dev_rx_intr_disable(0 /* port id */, queue_id /* queue id */);
	}
}

static int nic_init(int ip_thread_num, int tcp_thread_num, int max_epoll_wait_timeout_ms)
{
	LOG_DEBUG("nic_init: 1, begin, ip_thread_num: %d, tcp_thread_num: %d\n",
			  ip_thread_num, tcp_thread_num);
	{
		uint16_t nb_rxq = ip_thread_num;
		uint16_t nb_txq = 1 + tcp_thread_num;

		uint16_t nb_rxd = NUM_SLOT;
		uint16_t nb_txd = NUM_SLOT;
		assert((pktmbuf_pool = rte_pktmbuf_pool_create("mbuf_pool",
													   RTE_MAX(1 /* nb_ports */ * (nb_rxd + nb_txd + MAX_PKT_BURST + 1 * MEMPOOL_CACHE_SIZE), /* 8192 */ 65536 - 1),
													   MEMPOOL_CACHE_SIZE, 0, RTE_MBUF_DEFAULT_BUF_SIZE,
													   rte_socket_id())) != NULL);

		// pktmbuf_pool_tcp_tx = tcp_create_pktmbuf_pool_tcp_tx(tcp_thread_num);
		// assert(pktmbuf_pool_tcp_tx != NULL);

		// pktmbuf_pool_rx = tcp_create_pktmbuf_pool_rx(tcp_thread_num);
		// assert(pktmbuf_pool_rx != NULL);

		LOG_DEBUG("nic_init: 2\n");
		{
			struct rte_eth_dev_info dev_info;
			// struct rte_eth_conf local_port_conf = { 0 };
			struct rte_eth_conf local_port_conf = {
				.rxmode = {
					.mq_mode = RTE_ETH_MQ_RX_RSS, // 关键：启用多队列接收模式
					.split_hdr_size = 0,
				},
				.rx_adv_conf = {
					.rss_conf = {
						.rss_key = NULL,											  // 使用网卡默认的哈希Key，通常设为NULL
						.rss_hf = RTE_ETH_RSS_IP | RTE_ETH_RSS_TCP | RTE_ETH_RSS_UDP, // 指定对哪些字段做哈希
					},
				},
			};

			assert(rte_eth_dev_info_get(0 /* port id */, &dev_info) >= 0);

			if (max_epoll_wait_timeout_ms)
				local_port_conf.intr_conf.rxq = 1;

			local_port_conf.rx_adv_conf.rss_conf.rss_hf &= dev_info.flow_type_rss_offloads;

			LOG_DEBUG("nic_init: 3\n");
			// assert(rte_eth_dev_configure(0 /* port id */, 1 /* num queues */, 1 /* num queues */, &local_port_conf) >= 0);
			assert(rte_eth_dev_configure(0 /* port id */, nb_rxq /* num rx queues */, nb_txq /* num tx queues */, &local_port_conf) >= 0);

			LOG_DEBUG("nic_init: 4\n");
			assert(rte_eth_dev_adjust_nb_rx_tx_desc(0 /* port id */, &nb_rxd, &nb_txd) >= 0);

			LOG_DEBUG("nic_init: 5\n");
			for (int i = 0; i < nb_rxq; i++)
			{
				assert(rte_eth_rx_queue_setup(0 /* port id */, i /* queue */, nb_rxd,
											  rte_eth_dev_socket_id(0 /* port id */),
											  &dev_info.default_rxconf,
											  pktmbuf_pool) >= 0);
			}

			LOG_DEBUG("nic_init: 6\n");
			for (int i = 0; i < nb_txq; i++)
			{
				assert(rte_eth_tx_queue_setup(0 /* port id */, i /* queue */, nb_txd,
											  rte_eth_dev_socket_id(0 /* port id */),
											  &dev_info.default_txconf) >= 0);
			}

			LOG_DEBUG("nic_init: 7\n");
			assert(rte_eth_dev_start(0 /* port id */) >= 0);
			assert(rte_eth_promiscuous_enable(0 /* port id */) >= 0);

			LOG_DEBUG("nic_init: 8\n");
			if (max_epoll_wait_timeout_ms)
				assert(!rte_eth_dev_rx_intr_ctl_q(0 /* port id */, 0 /* queue */, RTE_EPOLL_PER_THREAD, RTE_INTR_EVENT_ADD, NULL));
		}
	}

	LOG_DEBUG("nic_init: 9, end\n");
	return 0;
}


static int server_port = 10000, num_conn = 1;

void server_mode_init()
{
	// LOG_DEBUG("main: 5.1\n");
	// 	{
	// 		size_t buflen = content_len + 256 /* for http hdr */;
	// 		char *content;
	// 		assert((httpbuf = (char *) malloc(buflen)) != NULL);
	// 		assert((content = (char *) malloc(content_len + 1)) != NULL);
	// 		memset(content, 'A', content_len);
	// 		content[content_len] = '\0';
	// 		httpdatalen = snprintf(httpbuf, buflen, "HTTP/1.1 200 OK\r\nContent-Length: %lu\r\nConnection: keep-alive\r\n\r\n%s", content_len, content);
	// 		free(content);
	// 		printf("http data length: %lu bytes\n", httpdatalen);
	// 	}

		LOG_DEBUG("main: 5.2\n");
		{
			struct tcp_pcb *tpcb, *_tpcb;
		
			LOG_DEBUG("main: 5.2.1\n");
			assert((_tpcb = tcp_new()) != NULL);
			
			LOG_DEBUG("main: 5.2.2\n");
			assert(tcp_bind(_tpcb, IP_ADDR_ANY, server_port) == ERR_OK);
			
			LOG_DEBUG("main: 5.2.3.\n");
			assert((tpcb = tcp_listen(_tpcb)) != NULL);
			
			LOG_DEBUG("main: 5.2.4\n");
			tcp_accept(tpcb, accept_handler);

			LOG_DEBUG("main: 5.2.5\n");
			tcp_ext_arg_set_callbacks(tpcb, 0, &tcp_ext_arg_cbs);
			tcp_ext_arg_set(tpcb, 0, NULL);
		
			LOG_DEBUG("main: 5.2.6\n");
		}

}

ip4_addr_t _srv_ip;

void client_mode_init()
{
	LOG_INFO("client_mode_init --------------- \n");
	int i;
	printf("%d concurrent connection(s)\n", num_conn);
	for (i = 0; i < num_conn; i++)
	{
		LOG_DEBUG("client_mode_init: 1\n");
		struct tcp_pcb *tpcb;
		assert((tpcb = tcp_new()) != NULL);
		{
			LOG_DEBUG("client_mode_init: 2\n");
			struct http_response *r;
			assert((r = (struct http_response *)malloc(sizeof(struct http_response))) != NULL);
			r->state = 0;
			r->cur = 0;
			tcp_arg(tpcb, (void *)r);
			tcp_ext_arg_set(tpcb, 0, (void *)r);
		}
		LOG_DEBUG("client_mode_init: 3\n");
		assert(tcp_connect(tpcb, &_srv_ip, server_port, connected_handler) == ERR_OK);
	}
	LOG_DEBUG("client_mode_init: 4\n");
}

bool mode_server = true;

void user_app_init()
{
	LOG_INFO("main: 5, %d, -------------------------\n", mode_server);
	if (mode_server) { /* server mode */

		server_mode_init();

	} else { /* client mode */

		// while (1) sleep(1);
		client_mode_init();
	}

}

int main(int argc, char *const *argv)
{
	struct netif _netif = { 0 };
	ip4_addr_t _addr, _mask, _gate/* , _srv_ip */;
	// size_t content_len = 1;
	// int server_port = 10000, num_conn = 1;
	// bool mode_server = true;
	// int max_epoll_wait_timeout_ms = 0;
	int ip_thread_num = 2;
	int tcp_thread_num = 4;

	LOG_DEBUG("main: 1\n");

	{
		int ret;
		assert((ret = rte_eal_init(argc, (char **) argv)) >= 0);
		argc -= ret;
		argv += ret;
	}

	LOG_DEBUG("main: 2\n");
	LOG_INFO("dev_count_avail: %d\n", rte_eth_dev_count_avail());

	assert(rte_eth_dev_count_avail() == 1);

	{
		int ch;
		bool _a = false, _g = false, _m = false;
		while ((ch = getopt(argc, argv, "a:c:e:g:i:j:l:m:p:s:")) != -1) {
			switch (ch) {
			case 'a':
				inet_pton(AF_INET, optarg, &_addr);
				_a = true;
				break;
			case 'c':
				num_conn = atoi(optarg);
				break;
			case 'e':
				assert(sscanf(optarg, "%d", &max_epoll_wait_timeout_ms) == 1);
				break;
			case 'g':
				inet_pton(AF_INET, optarg, &_gate);
				_g = true;
				break;
			case 'm':
				inet_pton(AF_INET, optarg, &_mask);
				_m = true;
				break;
			case 'l':
				// content_len = atol(optarg);
				break;
			case 'i':
			    ip_thread_num = atoi(optarg);
				break;
			case 'j':
			    tcp_thread_num = atoi(optarg);
				break;
			case 'p':
				server_port = atoi(optarg);
				break;
			case 's':
				inet_pton(AF_INET, optarg, &_srv_ip);
				mode_server = false;
				break;
			default:
				assert(0);
				break;
			}
		}
		assert(_a && _g && _m);
	}

	LOG_INFO("main: 3, ip_thread_num: %d, tcp_thread_num: %d\n", ip_thread_num, tcp_thread_num);
	// {
	// 	uint16_t nb_rxd = NUM_SLOT;
	// 	uint16_t nb_txd = NUM_SLOT;
	// 	assert((pktmbuf_pool = rte_pktmbuf_pool_create("mbuf_pool",
	// 				RTE_MAX(1 /* nb_ports */ * (nb_rxd + nb_txd + MAX_PKT_BURST + 1 * MEMPOOL_CACHE_SIZE), 8192),
	// 				MEMPOOL_CACHE_SIZE, 0, RTE_MBUF_DEFAULT_BUF_SIZE,
	// 				rte_socket_id())) != NULL);

	// 	{
	// 		struct rte_eth_dev_info dev_info;
	// 		struct rte_eth_conf local_port_conf = { 0 };

	// 		assert(rte_eth_dev_info_get(0 /* port id */, &dev_info) >= 0);

	// 		if (max_epoll_wait_timeout_ms)
	// 			local_port_conf.intr_conf.rxq = 1;

	// 		assert(rte_eth_dev_configure(0 /* port id */, 1 /* num queues */, 1 /* num queues */, &local_port_conf) >= 0);

	// 		assert(rte_eth_dev_adjust_nb_rx_tx_desc(0 /* port id */, &nb_rxd, &nb_txd) >= 0);

	// 		assert(rte_eth_rx_queue_setup(0 /* port id */, 0 /* queue */, nb_rxd,
	// 					rte_eth_dev_socket_id(0 /* port id */),
	// 					&dev_info.default_rxconf,
	// 					pktmbuf_pool) >= 0);

	// 		assert(rte_eth_tx_queue_setup(0 /* port id */, 0 /* queue */, nb_txd,
	// 					rte_eth_dev_socket_id(0 /* port id */),
	// 					&dev_info.default_txconf) >= 0);

	// 		assert(rte_eth_dev_start(0 /* port id */) >= 0);
	// 		assert(rte_eth_promiscuous_enable(0 /* port id */) >= 0);

	// 		if (max_epoll_wait_timeout_ms)
	// 			assert(!rte_eth_dev_rx_intr_ctl_q(0 /* port id */, 0 /* queue */, RTE_EPOLL_PER_THREAD, RTE_INTR_EVENT_ADD, NULL));
	// 	}
	// }


	nic_init(ip_thread_num, tcp_thread_num, max_epoll_wait_timeout_ms);


	/* setting up lwip */
	LOG_DEBUG("main: 4\n");
	{
		lwip_init();
		assert(netif_add(&_netif, &_addr, &_mask, &_gate, NULL, if_init, ethernet_input) != NULL);
		netif_set_default(&_netif);
		netif_set_link_up(&_netif);
		netif_set_up(&_netif);
	}


	// thread framework init
	thread_framework_init(ip_thread_num, tcp_thread_num, &_netif);



	// LOG_DEBUG("main: 5\n");
	// if (mode_server) { /* server mode */
		
	// 	// LOG_DEBUG("main: 5.1\n");
	// 	// {
	// 	// 	size_t buflen = content_len + 256 /* for http hdr */;
	// 	// 	char *content;
	// 	// 	assert((httpbuf = (char *) malloc(buflen)) != NULL);
	// 	// 	assert((content = (char *) malloc(content_len + 1)) != NULL);
	// 	// 	memset(content, 'A', content_len);
	// 	// 	content[content_len] = '\0';
	// 	// 	httpdatalen = snprintf(httpbuf, buflen, "HTTP/1.1 200 OK\r\nContent-Length: %lu\r\nConnection: keep-alive\r\n\r\n%s", content_len, content);
	// 	// 	free(content);
	// 	// 	printf("http data length: %lu bytes\n", httpdatalen);
	// 	// }

	// 	// LOG_DEBUG("main: 5.2\n");
	// 	// {
	// 	// 	struct tcp_pcb *tpcb, *_tpcb;
		
	// 	// 	LOG_DEBUG("main: 5.2.1\n");
	// 	// 	assert((_tpcb = tcp_new()) != NULL);
			
	// 	// 	LOG_DEBUG("main: 5.2.2\n");
	// 	// 	assert(tcp_bind(_tpcb, IP_ADDR_ANY, server_port) == ERR_OK);
			
	// 	// 	LOG_DEBUG("main: 5.2.3.\n");
	// 	// 	assert((tpcb = tcp_listen(_tpcb)) != NULL);
			
	// 	// 	LOG_DEBUG("main: 5.2.4\n");
	// 	// 	tcp_accept(tpcb, accept_handler);

	// 	// 	LOG_DEBUG("main: 5.2.5\n");
	// 	// 	tcp_ext_arg_set_callbacks(tpcb, 0, &tcp_ext_arg_cbs);
	// 	// 	tcp_ext_arg_set(tpcb, 0, NULL);
		
	// 	// 	LOG_DEBUG("main: 5.2.6\n");
	// 	// }

	// 	server_mode_init();

	// } else { /* client mode */
	// 	// int i;
	// 	// printf("%d concurrent connection(s)\n", num_conn);
	// 	// for (i = 0; i < num_conn; i++) {
	// 	// 	struct tcp_pcb *tpcb;
	// 	// 	assert((tpcb = tcp_new()) != NULL);
	// 	// 	{
	// 	// 		struct http_response *r;
	// 	// 		assert((r = (struct http_response *) malloc(sizeof(struct http_response))) != NULL);
	// 	// 		r->state = 0;
	// 	// 		r->cur = 0;
	// 	// 		tcp_arg(tpcb, (void *) r);
	// 	// 		tcp_ext_arg_set(tpcb, 0, (void *) r);
	// 	// 	}
	// 	// 	assert(tcp_connect(tpcb, &_srv_ip, server_port, connected_handler) == ERR_OK);
	// 	// }
	// 	client_mode_init();
	// }
	
	// move app_init to tcp_thread_run, id = 0
	// app_init();

	LOG_DEBUG("main: 6\n");
	printf("-- application has started --\n");

	/* primary loop */
	{
		unsigned long prev_ts = 0;
		
		LOG_DEBUG("main: 7\n");
		while (1) {

			// // LOG_DEBUG("main: 7.1\n");
			// struct rte_mbuf *rx_mbufs[MAX_PKT_BURST];
			// unsigned short i, nb_rx = rte_eth_rx_burst(0 /* port id */, 0 /* queue id */, rx_mbufs, MAX_PKT_BURST);

			// // LOG_DEBUG("main: 7.2\n"); 
			// for (i = 0; i < nb_rx; i++) {
			// 	LOG_DEBUG("main: 7.3\n");
			// 	{
			// 		LOG_DEBUG("main: 7.4\n");
			// 		struct pbuf *p;
			// 		assert((p = pbuf_alloc(PBUF_RAW, rte_pktmbuf_pkt_len(rx_mbufs[i]), PBUF_POOL)) != NULL);

			// 		LOG_DEBUG("main: 7.5\n");
			// 		pbuf_take(p, rte_pktmbuf_mtod(rx_mbufs[i], void *), rte_pktmbuf_pkt_len(rx_mbufs[i]));
					
			// 		LOG_DEBUG("main: 7.6\n");
			// 		p->len = p->tot_len = rte_pktmbuf_pkt_len(rx_mbufs[i]);
			// 		assert(_netif.input(p, &_netif) == ERR_OK);
				
			// 		LOG_DEBUG("main: 7.7\n");
			// 	}
			// 	rte_pktmbuf_free(rx_mbufs[i]);
			// }

			// unsigned short nb_rx = netif_poll_once(&_netif, 0);

			// LOG_INFO("main: 7.8\n");
			// tx_flush();
			sleep(1);

			// LOG_DEBUG("main: 7.9\n");
			// sys_check_timeouts();

			LOG_DEBUG("main: 7.10\n");
			{
				unsigned long now = ({ struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts); (ts.tv_sec * 1000000000UL + ts.tv_nsec); });
				if (now - prev_ts > 1000000000UL) {
					printf("[%s]: %10lu Requests/sec  (rx %11lu bps, tx %11lu bbs)\n",
							(mode_server ? "server" : "client"), io_stat[0], io_stat[1] * 8, io_stat[2] * 8);
					memset(io_stat, 0, sizeof(io_stat));

					for (int tid = 0; tid < g_tcp_thread_num; tid++)
					{
						struct tcp_thread_ctx *ctx = &tcp_thread_ctxs[tid];

						LOG_INFO("tid: %d, loop_state: %d\n", tid, ctx->loop_state);
					}

					uint64_t recv_pkt_tot = 0, recv_pkt_byte_tot = 0;
					for (int i = 0; i < g_tcp_thread_num; i++)
					{   // 
						recv_pkt_tot += recv_pkt_cnt[i];
						recv_pkt_cnt[i] = 0;
						recv_pkt_byte_tot += recv_pkt_byte_cnt[i];
						recv_pkt_byte_cnt[i] = 0;
					}

					double duration = (double)(now - prev_ts) / 1000000000;
					double recv_pkt_per_sec = (double)recv_pkt_tot / duration;
					double recv_pkt_byte_per_sec = (double)recv_pkt_byte_tot / duration;

					LOG_INFO("duration: %lf, recv_pkt_per_sec: %lf, recv_pkt_byte_per_sec: %lf\n",
						duration, recv_pkt_per_sec, recv_pkt_byte_per_sec);

					
					uint64_t tcp_input_frontend_pkt_cnt_tot = 0;
					for (int i = 0; i < g_ip_thread_num; i++)
					{
						tcp_input_frontend_pkt_cnt_tot += tcp_input_frontend_pkt_cnt[i];
						tcp_input_frontend_pkt_cnt[i] = 0;
					}

					double tcp_input_frontend_pkt_cnt_per_sec = (double)tcp_input_frontend_pkt_cnt_tot / duration;
					LOG_INFO("tcp_input_frontend_pkt_cnt_per_sec: %lf\n", tcp_input_frontend_pkt_cnt_per_sec);


					prev_ts = now;
				}
			}


			// LOG_DEBUG("main: 7.11\n");
			// if (!nb_rx && max_epoll_wait_timeout_ms) {
				
			// 	LOG_DEBUG("main: 7.12\n");
			// 	assert(!rte_eth_dev_rx_intr_enable(0 /* port id */, 0 /* queue id */));
			// 	{
			// 		struct rte_epoll_event ev;
			// 		(void) rte_epoll_wait(RTE_EPOLL_PER_THREAD, &ev, 1, max_epoll_wait_timeout_ms < 0 ? 100 : (max_epoll_wait_timeout_ms > 100 ? 100 : max_epoll_wait_timeout_ms));
			// 	}

			// 	LOG_DEBUG("main: 7.13\n");
			// 	rte_eth_dev_rx_intr_disable(0 /* port id */, 0 /* queue id */);
			// }

		    // usleep(1);
			// LOG_DEBUG("main: 7.14\n");
		}
	}

	// for (int i = 0; i < g_tcp_thread_num; i++)
	// {
	// 	pthread_join(tcp_thread_ctxs[i].pthread_ctx, NULL);
	// }



	LOG_DEBUG("main: 8\n");
	return 0;
}
