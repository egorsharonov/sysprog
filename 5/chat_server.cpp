#include "chat.h"
#include "chat_server.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>

#include <algorithm>
#include <deque>
#include <stdlib.h>
#include <string>
#include <string_view>
#include <vector>
#include <string.h>
#include <unistd.h>

struct chat_peer {
	/** Client's socket. To read/write messages. */
	int socket;
	/** Output buffer. */
	std::string out_buffer;
	size_t out_offset = 0;
	std::string in_buffer;
	size_t in_offset = 0;
	std::string author;
	bool has_author = false;
};

struct chat_server {
	/** Listening socket. To accept new clients. */
	int socket = -1;
	/** Array of peers. */
	int epoll_fd = -1;
	std::vector<chat_peer *> peers;
	std::deque<chat_message *> messages;
	std::string feed_buffer;
};

static uint32_t
be32_decode(const char *data)
{
	uint32_t value = 0;
	memcpy(&value, data, sizeof(value));
	return ntohl(value);
}

static void
be32_append(std::string &buf, uint32_t value)
{
	uint32_t net = htonl(value);
	buf.append((const char *)&net, sizeof(net));
}

static void
append_frame(std::string &buf, std::string_view author, std::string_view data)
{
	be32_append(buf, (uint32_t)author.size());
	be32_append(buf, (uint32_t)data.size());
	buf.append(author.data(), author.size());
	buf.append(data.data(), data.size());
}

static bool
extract_frame(std::string &buf, size_t &offset, std::string &author, std::string &data)
{
	if (buf.size() - offset < sizeof(uint32_t) * 2)
		return false;
	const char *ptr = buf.data() + offset;
	uint32_t author_len = be32_decode(ptr);
	uint32_t data_len = be32_decode(ptr + sizeof(uint32_t));
	size_t total = sizeof(uint32_t) * 2 + (size_t)author_len + (size_t)data_len;
	if (buf.size() - offset < total)
		return false;
	ptr += sizeof(uint32_t) * 2;
	author.assign(ptr, author_len);
	ptr += author_len;
	data.assign(ptr, data_len);
	offset += total;
	if (offset == buf.size()) {
		buf.clear();
		offset = 0;
	} else if (offset > 65536) {
		buf.erase(0, offset);
		offset = 0;
	}
	return true;
}

static std::string
trim_message(std::string_view data)
{
	size_t begin = 0;
	size_t end = data.size();
	while (begin < end && isspace((unsigned char)data[begin]) != 0)
		++begin;
	while (end > begin && isspace((unsigned char)data[end - 1]) != 0)
		--end;
	return std::string(data.substr(begin, end - begin));
}

static int
socket_make_non_blocking(int fd)
{
	int flags = fcntl(fd, F_GETFL, 0);
	if (flags < 0)
		return -1;
	if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0)
		return -1;
	return 0;
}

static int
socket_flush(int fd, std::string &buffer, size_t &offset)
{
	while (offset < buffer.size()) {
		const char *ptr = buffer.data() + offset;
		size_t left = buffer.size() - offset;
#ifdef MSG_NOSIGNAL
		ssize_t rc = send(fd, ptr, left, MSG_NOSIGNAL);
#else
		ssize_t rc = send(fd, ptr, left, 0);
#endif
		if (rc > 0) {
			offset += (size_t)rc;
			continue;
		}
		if (rc < 0 && errno == EINTR)
			continue;
		if (rc < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
			break;
		return -1;
	}
	if (offset == buffer.size()) {
		buffer.clear();
		offset = 0;
	}
	return 0;
}

static void
server_remove_peer(struct chat_server *server, struct chat_peer *peer)
{
	if (server->epoll_fd >= 0)
		epoll_ctl(server->epoll_fd, EPOLL_CTL_DEL, peer->socket, nullptr);
	close(peer->socket);
	for (size_t i = 0; i < server->peers.size(); ++i) {
		if (server->peers[i] == peer) {
			server->peers[i] = server->peers.back();
			server->peers.pop_back();
			break;
		}
	}
	delete peer;
}

static bool
peer_queue_message(struct chat_peer *peer, std::string_view author, std::string_view data)
{
	append_frame(peer->out_buffer, author, data);
	return socket_flush(peer->socket, peer->out_buffer, peer->out_offset) == 0;
}

static void
server_broadcast(struct chat_server *server, const struct chat_peer *sender,
		 std::string_view author, std::string_view data)
{
	size_t i = 0;
	while (i < server->peers.size()) {
		chat_peer *peer = server->peers[i];
		if (sender != nullptr && peer == sender) {
			++i;
			continue;
		}
		if (!peer_queue_message(peer, author, data)) {
			server_remove_peer(server, peer);
			continue;
		}
		++i;
	}
}

static bool
server_peer_read(struct chat_server *server, struct chat_peer *peer)
{
	char tmp[65536];
	while (true) {
		ssize_t rc = recv(peer->socket, tmp, sizeof(tmp), 0);
		if (rc > 0) {
			peer->in_buffer.append(tmp, (size_t)rc);
			continue;
		}
		if (rc == 0)
			return false;
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			break;
		return false;
	}

	std::string author;
	std::string data;
	while (extract_frame(peer->in_buffer, peer->in_offset, author, data)) {
		if (!peer->has_author && !author.empty() && data.empty()) {
			peer->author = std::move(author);
			peer->has_author = true;
			author.clear();
			data.clear();
			continue;
		}
		if (data.empty()) {
			author.clear();
			data.clear();
			continue;
		}
		chat_message *msg = new chat_message();
		msg->data = data;
#if NEED_AUTHOR
		msg->author = peer->has_author ? peer->author : std::string();
#endif
		server->messages.push_back(msg);

#if NEED_AUTHOR
		std::string_view out_author = peer->has_author ?
			std::string_view(peer->author) : std::string_view();
#else
		std::string_view out_author;
#endif
		server_broadcast(server, peer, out_author, data);
		author.clear();
		data.clear();
	}
	return true;
}

static int
server_accept_new_clients(struct chat_server *server)
{
	while (true) {
		int client_fd = accept(server->socket, nullptr, nullptr);
		if (client_fd < 0) {
			if (errno == EAGAIN || errno == EWOULDBLOCK)
				return 0;
			if (errno == EINTR)
				continue;
			return CHAT_ERR_SYS;
		}
		if (socket_make_non_blocking(client_fd) != 0) {
			close(client_fd);
			continue;
		}
		chat_peer *peer = new chat_peer();
		peer->socket = client_fd;
		struct epoll_event peer_event;
		memset(&peer_event, 0, sizeof(peer_event));
		peer_event.events = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;
		peer_event.data.ptr = peer;
		if (epoll_ctl(server->epoll_fd, EPOLL_CTL_ADD, client_fd, &peer_event) != 0) {
			close(client_fd);
			delete peer;
			continue;
		}
		server->peers.push_back(peer);
	}
}

static int
feed_text(struct chat_server *server, std::string &input, std::string_view author)
{
	while (true) {
		size_t eol = input.find('\n');
		if (eol == std::string::npos)
			break;
		std::string line = input.substr(0, eol);
		input.erase(0, eol + 1);
		std::string msg = trim_message(line);
		if (msg.empty())
			continue;
		server_broadcast(server, nullptr, author, msg);
	}
	return 0;
}

struct chat_server *
chat_server_new(void)
{
	return new chat_server();
}

void
chat_server_delete(struct chat_server *server)
{
	for (chat_peer *peer : server->peers) {
		if (server->epoll_fd >= 0)
			epoll_ctl(server->epoll_fd, EPOLL_CTL_DEL, peer->socket, nullptr);
		close(peer->socket);
		delete peer;
	}
	for (chat_message *msg : server->messages)
		delete msg;
	if (server->socket >= 0)
		close(server->socket);
	if (server->epoll_fd >= 0)
		close(server->epoll_fd);
	delete server;
}

int
chat_server_listen(struct chat_server *server, uint16_t port)
{
	if (server->socket >= 0)
		return CHAT_ERR_ALREADY_STARTED;
	struct sockaddr_in addr;
	memset(&addr, 0, sizeof(addr));
	addr.sin_family = AF_INET;
	addr.sin_port = htons(port);
	/* Listen on all IPs of this machine. */
	addr.sin_addr.s_addr = htonl(INADDR_ANY);
	int sock = socket(AF_INET, SOCK_STREAM, 0);
	if (sock < 0)
		return CHAT_ERR_SYS;
	int yes = 1;
	setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
	if (socket_make_non_blocking(sock) != 0) {
		close(sock);
		return CHAT_ERR_SYS;
	}
	if (bind(sock, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
		int saved_errno = errno;
		close(sock);
		if (saved_errno == EADDRINUSE)
			return CHAT_ERR_PORT_BUSY;
		return CHAT_ERR_SYS;
	}
	if (listen(sock, SOMAXCONN) != 0) {
		close(sock);
		return CHAT_ERR_SYS;
	}
	int epoll_fd = epoll_create1(EPOLL_CLOEXEC);
	if (epoll_fd < 0) {
		close(sock);
		return CHAT_ERR_SYS;
	}
	struct epoll_event event;
	memset(&event, 0, sizeof(event));
	event.events = EPOLLIN | EPOLLET;
	event.data.ptr = server;
	if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, sock, &event) != 0) {
		close(epoll_fd);
		close(sock);
		return CHAT_ERR_SYS;
	}
	server->socket = sock;
	server->epoll_fd = epoll_fd;
	return 0;
}

struct chat_message *
chat_server_pop_next(struct chat_server *server)
{
	if (server->messages.empty())
		return nullptr;
	chat_message *msg = server->messages.front();
	server->messages.pop_front();
	return msg;
}

int
chat_server_update(struct chat_server *server, double timeout)
{
	if (server->socket < 0)
		return CHAT_ERR_NOT_STARTED;
	int timeout_ms = -1;
	if (timeout >= 0)
		timeout_ms = (int)(timeout * 1000.0 + 0.999999);

	struct epoll_event events[64];
	int rc = epoll_wait(server->epoll_fd, events, 64, timeout_ms);
	if (rc < 0)
		return CHAT_ERR_SYS;
	if (rc == 0)
		return CHAT_ERR_TIMEOUT;

	for (int i = 0; i < rc; ++i) {
		struct epoll_event *event = &events[i];
		if (event->data.ptr == server) {
			int accept_rc = server_accept_new_clients(server);
			if (accept_rc != 0)
				return accept_rc;
			continue;
		}
		auto *peer = (chat_peer *)event->data.ptr;
		bool is_ok = true;
		if ((event->events & EPOLLIN) != 0)
			is_ok = server_peer_read(server, peer);
		if (is_ok && (event->events & EPOLLOUT) != 0 &&
		    peer->out_offset < peer->out_buffer.size())
			is_ok = socket_flush(peer->socket, peer->out_buffer, peer->out_offset) == 0;
		if (is_ok && (event->events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP)) != 0)
			is_ok = false;
		if (!is_ok)
			server_remove_peer(server, peer);
	}
	return 0;
}

int
chat_server_get_descriptor(const struct chat_server *server)
{
#if NEED_SERVER_FEED
	return server->epoll_fd;
#endif
	(void)server;
	return -1;
}

int
chat_server_get_socket(const struct chat_server *server)
{
	return server->socket;
}

int
chat_server_get_events(const struct chat_server *server)
{
	if (server->socket < 0)
		return 0;
	int events = CHAT_EVENT_INPUT;
	for (const chat_peer *peer : server->peers) {
		if (peer->out_offset < peer->out_buffer.size()) {
			events |= CHAT_EVENT_OUTPUT;
			break;
		}
	}
	return events;
}

int
chat_server_feed(struct chat_server *server, const char *msg, uint32_t msg_size)
{
#if NEED_SERVER_FEED
	if (server->socket < 0)
		return CHAT_ERR_NOT_STARTED;
	int rc = server_accept_new_clients(server);
	if (rc != 0)
		return rc;
	server->feed_buffer.append(msg, msg_size);
#if NEED_AUTHOR
	std::string_view author = "server";
#else
	std::string_view author;
#endif
	feed_text(server, server->feed_buffer, author);
	return 0;
#endif
	(void)server;
	(void)msg;
	(void)msg_size;
	return CHAT_ERR_NOT_IMPLEMENTED;
}
