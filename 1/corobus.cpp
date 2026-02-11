#include "corobus.h"

#include "libcoro.h"
#include "rlist.h"

#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include <deque>
#include <algorithm>

/**
 * One coroutine waiting to be woken up in a list of other
 * suspended coros.
 */
struct wakeup_entry {
	struct rlist base;
	struct coro *coro;
};

/** A queue of suspended coros waiting to be woken up. */
struct wakeup_queue {
	struct rlist coros;
};

static void
wakeup_queue_init(struct wakeup_queue *queue)
{
	rlist_create(&queue->coros);
}

static void
wakeup_queue_suspend_this(struct wakeup_queue *queue)
{
	struct wakeup_entry entry;
	rlist_create(&entry.base);
	entry.coro = coro_this();
	rlist_add_tail_entry(&queue->coros, &entry, base);
	coro_suspend();
	if (entry.base.next != &entry.base || entry.base.prev != &entry.base)
		rlist_del_entry(&entry, base);
}

static void
wakeup_queue_wakeup_first(struct wakeup_queue *queue)
{
	if (rlist_empty(&queue->coros))
		return;
	struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
		struct wakeup_entry, base);
	rlist_del_entry(entry, base);
	coro_wakeup(entry->coro);
}

static void
wakeup_queue_wakeup_all(struct wakeup_queue *queue)
{
	while (!rlist_empty(&queue->coros)) {
		struct wakeup_entry *entry = rlist_first_entry(&queue->coros,
			struct wakeup_entry, base);
		rlist_del_entry(entry, base);
		coro_wakeup(entry->coro);
	}
}

static void
wakeup_queue_wakeup_n(struct wakeup_queue *queue, size_t count)
{
	for (size_t i = 0; i < count; ++i) {
		if (rlist_empty(&queue->coros))
			return;
		wakeup_queue_wakeup_first(queue);
	}
}

struct coro_bus_channel {
	/** Channel max capacity. */
	size_t size_limit;
	/** Coroutines waiting until the channel is not full. */
	struct wakeup_queue send_queue;
	/** Coroutines waiting until the channel is not empty. */
	struct wakeup_queue recv_queue;
	/** Message queue. */
	std::deque<unsigned> data;
};

struct coro_bus {
	struct coro_bus_channel **channels;
	unsigned long long *channel_gens;
	int channel_count;
};

static enum coro_bus_error_code global_error = CORO_BUS_ERR_NONE;

enum coro_bus_error_code
coro_bus_errno(void)
{
	return global_error;
}

void
coro_bus_errno_set(enum coro_bus_error_code err)
{
	global_error = err;
}

static struct coro_bus_channel *
channel_get(struct coro_bus *bus, int channel)
{
	if (bus == NULL || channel < 0 || channel >= bus->channel_count)
		return NULL;
	return bus->channels[channel];
}

static unsigned long long
channel_gen_get(struct coro_bus *bus, int channel)
{
	if (bus == NULL || channel < 0 || channel >= bus->channel_count)
		return 0;
	return bus->channel_gens[channel];
}

static bool
channel_is_same(struct coro_bus *bus, int channel, unsigned long long gen)
{
	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL)
		return false;
	return channel_gen_get(bus, channel) == gen;
}

static void
channel_init(struct coro_bus_channel *channel, size_t size_limit)
{
	channel->size_limit = size_limit;
	wakeup_queue_init(&channel->send_queue);
	wakeup_queue_init(&channel->recv_queue);
}

struct coro_bus *
coro_bus_new(void)
{
	struct coro_bus *bus = new coro_bus;
	bus->channels = NULL;
	bus->channel_gens = NULL;
	bus->channel_count = 0;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return bus;
}

void
coro_bus_delete(struct coro_bus *bus)
{
	if (bus == NULL)
		return;
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *channel = bus->channels[i];
		if (channel == NULL)
			continue;
		assert(rlist_empty(&channel->send_queue.coros));
		assert(rlist_empty(&channel->recv_queue.coros));
		delete channel;
		bus->channels[i] = NULL;
	}
	delete[] bus->channels;
	delete[] bus->channel_gens;
	delete bus;
}

int
coro_bus_channel_open(struct coro_bus *bus, size_t size_limit)
{
	if (bus == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	for (int i = 0; i < bus->channel_count; ++i) {
		if (bus->channels[i] == NULL) {
			struct coro_bus_channel *channel = new coro_bus_channel;
			channel_init(channel, size_limit);
			bus->channels[i] = channel;
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return i;
		}
	}
	int new_count = bus->channel_count + 1;
	struct coro_bus_channel **new_channels = new coro_bus_channel *[new_count];
	unsigned long long *new_gens = new unsigned long long[new_count];
	for (int i = 0; i < new_count; ++i)
		new_channels[i] = NULL;
	for (int i = 0; i < bus->channel_count; ++i)
		new_channels[i] = bus->channels[i];
	for (int i = 0; i < bus->channel_count; ++i)
		new_gens[i] = bus->channel_gens[i];
	new_gens[new_count - 1] = 1;
	delete[] bus->channels;
	delete[] bus->channel_gens;
	bus->channels = new_channels;
	bus->channel_gens = new_gens;
	bus->channel_count = new_count;

	struct coro_bus_channel *channel = new coro_bus_channel;
	channel_init(channel, size_limit);
	bus->channels[new_count - 1] = channel;
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return new_count - 1;
}

void
coro_bus_channel_close(struct coro_bus *bus, int channel)
{
	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL)
		return;
	bus->channels[channel] = NULL;
	bus->channel_gens[channel] += 1;
	wakeup_queue_wakeup_all(&ch->send_queue);
	wakeup_queue_wakeup_all(&ch->recv_queue);
	delete ch;
}

int
coro_bus_send(struct coro_bus *bus, int channel, unsigned data)
{
	for (;;) {
		int rc = coro_bus_try_send(bus, channel, data);
		if (rc == 0) {
			return 0;
		}
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
			return -1;
		struct coro_bus_channel *ch = channel_get(bus, channel);
		unsigned long long gen = channel_gen_get(bus, channel);
		if (ch == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		wakeup_queue_suspend_this(&ch->send_queue);
		if (!channel_is_same(bus, channel, gen)) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
	}
}

int
coro_bus_try_send(struct coro_bus *bus, int channel, unsigned data)
{
	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	if (ch->data.size() >= ch->size_limit) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	ch->data.push_back(data);
	wakeup_queue_wakeup_first(&ch->recv_queue);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

int
coro_bus_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	for (;;) {
		int rc = coro_bus_try_recv(bus, channel, data);
		if (rc == 0)
			return 0;
		if (coro_bus_errno() == CORO_BUS_ERR_NO_CHANNEL)
			return -1;
		struct coro_bus_channel *ch = channel_get(bus, channel);
		unsigned long long gen = channel_gen_get(bus, channel);
		if (ch == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		wakeup_queue_suspend_this(&ch->recv_queue);
		if (!channel_is_same(bus, channel, gen)) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
	}
}

int
coro_bus_try_recv(struct coro_bus *bus, int channel, unsigned *data)
{
	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	if (ch->data.empty()) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	*data = ch->data.front();
	ch->data.pop_front();
	wakeup_queue_wakeup_first(&ch->send_queue);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

#if NEED_BROADCAST

int
coro_bus_broadcast(struct coro_bus *bus, unsigned data)
{
	for (;;) {
		if (bus == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		bool has_channels = false;
		bool has_full = false;
		struct coro_bus_channel *block_channel = NULL;
		for (int i = 0; i < bus->channel_count; ++i) {
			struct coro_bus_channel *ch = bus->channels[i];
			if (ch == NULL)
				continue;
			has_channels = true;
			if (ch->data.size() >= ch->size_limit) {
				has_full = true;
				block_channel = ch;
				break;
			}
		}
		if (!has_channels) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		if (!has_full) {
			for (int i = 0; i < bus->channel_count; ++i) {
				struct coro_bus_channel *ch = bus->channels[i];
				if (ch == NULL)
					continue;
				ch->data.push_back(data);
				wakeup_queue_wakeup_first(&ch->recv_queue);
			}
			coro_bus_errno_set(CORO_BUS_ERR_NONE);
			return 0;
		}
		wakeup_queue_suspend_this(&block_channel->send_queue);
	}
}

int
coro_bus_try_broadcast(struct coro_bus *bus, unsigned data)
{
	if (bus == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	bool has_channels = false;
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *ch = bus->channels[i];
		if (ch == NULL)
			continue;
		has_channels = true;
		if (ch->data.size() >= ch->size_limit) {
			coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
			return -1;
		}
	}
	if (!has_channels) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	for (int i = 0; i < bus->channel_count; ++i) {
		struct coro_bus_channel *ch = bus->channels[i];
		if (ch == NULL)
			continue;
		ch->data.push_back(data);
		wakeup_queue_wakeup_first(&ch->recv_queue);
	}
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return 0;
}

#endif

#if NEED_BATCH

int
coro_bus_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	if (count == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return 0;
	}
	for (;;) {
		struct coro_bus_channel *ch = channel_get(bus, channel);
		if (ch == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		size_t free_space = ch->size_limit - ch->data.size();
		if (free_space == 0) {
			unsigned long long gen = channel_gen_get(bus, channel);
			wakeup_queue_suspend_this(&ch->send_queue);
			if (!channel_is_same(bus, channel, gen)) {
				coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
				return -1;
			}
			continue;
		}
		unsigned to_send = std::min<unsigned>(count, (unsigned)free_space);
		for (unsigned i = 0; i < to_send; ++i)
			ch->data.push_back(data[i]);
		wakeup_queue_wakeup_n(&ch->recv_queue, to_send);
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return (int)to_send;
	}
}

int
coro_bus_try_send_v(struct coro_bus *bus, int channel, const unsigned *data, unsigned count)
{
	if (count == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return 0;
	}
	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	size_t free_space = ch->size_limit - ch->data.size();
	if (free_space == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	unsigned to_send = std::min<unsigned>(count, (unsigned)free_space);
	for (unsigned i = 0; i < to_send; ++i)
		ch->data.push_back(data[i]);
	wakeup_queue_wakeup_n(&ch->recv_queue, to_send);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return (int)to_send;
}

int
coro_bus_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	if (capacity == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return 0;
	}
	for (;;) {
		struct coro_bus_channel *ch = channel_get(bus, channel);
		if (ch == NULL) {
			coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
			return -1;
		}
		if (ch->data.empty()) {
			unsigned long long gen = channel_gen_get(bus, channel);
			wakeup_queue_suspend_this(&ch->recv_queue);
			if (!channel_is_same(bus, channel, gen)) {
				coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
				return -1;
			}
			continue;
		}
		unsigned to_recv = std::min<unsigned>(capacity, (unsigned)ch->data.size());
		for (unsigned i = 0; i < to_recv; ++i) {
			data[i] = ch->data.front();
			ch->data.pop_front();
		}
		wakeup_queue_wakeup_n(&ch->send_queue, to_recv);
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return (int)to_recv;
	}
}

int
coro_bus_try_recv_v(struct coro_bus *bus, int channel, unsigned *data, unsigned capacity)
{
	if (capacity == 0) {
		coro_bus_errno_set(CORO_BUS_ERR_NONE);
		return 0;
	}
	struct coro_bus_channel *ch = channel_get(bus, channel);
	if (ch == NULL) {
		coro_bus_errno_set(CORO_BUS_ERR_NO_CHANNEL);
		return -1;
	}
	if (ch->data.empty()) {
		coro_bus_errno_set(CORO_BUS_ERR_WOULD_BLOCK);
		return -1;
	}
	unsigned to_recv = std::min<unsigned>(capacity, (unsigned)ch->data.size());
	for (unsigned i = 0; i < to_recv; ++i) {
		data[i] = ch->data.front();
		ch->data.pop_front();
	}
	wakeup_queue_wakeup_n(&ch->send_queue, to_recv);
	coro_bus_errno_set(CORO_BUS_ERR_NONE);
	return (int)to_recv;
}

#endif
