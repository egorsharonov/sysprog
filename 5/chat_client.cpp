#include "chat.h"
#include "chat_client.h"

#include <arpa/inet.h>
#include <ctype.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <poll.h>
#include <sys/socket.h>

#include <algorithm>
#include <cstring>
#include <deque>
#include <stdlib.h>
#include <string>
#include <string_view>
#include <unistd.h>

struct chat_client {
	/** Socket connected to the server. */
	int socket = -1;
	/** Array of received messages. */
	std::deque<chat_message *> messages;
	/** Output buffer. */
	std::string out_buffer;
	size_t out_offset = 0;
	std::string in_buffer;
	size_t in_offset = 0;
	std::string feed_buffer;
	std::string name;
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
		return CHAT_ERR_SYS;
	}
	if (offset == buffer.size()) {
		buffer.clear();
		offset = 0;
	}
	return 0;
}

static int
feed_text(std::string &input, std::string &output)
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
		append_frame(output, std::string_view(), msg);
	}
	return 0;
}

static int
socket_read_messages(struct chat_client *client)
{
	char tmp[65536];
	while (true) {
		ssize_t rc = recv(client->socket, tmp, sizeof(tmp), 0);
		if (rc > 0) {
			client->in_buffer.append(tmp, (size_t)rc);
			continue;
		}
		if (rc == 0)
			return CHAT_ERR_SYS;
		if (errno == EINTR)
			continue;
		if (errno == EAGAIN || errno == EWOULDBLOCK)
			break;
		return CHAT_ERR_SYS;
	}
	std::string author;
	std::string data;
	while (extract_frame(client->in_buffer, client->in_offset, author, data)) {
		if (data.empty())
			continue;
		chat_message *msg = new chat_message();
		msg->data = std::move(data);
#if NEED_AUTHOR
		msg->author = std::move(author);
#endif
		client->messages.push_back(msg);
		author.clear();
		data.clear();
	}
	return 0;
}

struct chat_client *
chat_client_new(std::string_view name)
{
	chat_client *client = new chat_client();
	client->name = std::string(name);
	return client;
}

void
chat_client_delete(struct chat_client *client)
{
	if (client->socket >= 0)
		close(client->socket);
	for (chat_message *msg : client->messages)
		delete msg;
	delete client;
}

int
chat_client_connect(struct chat_client *client, std::string_view addr)
{
	if (client->socket >= 0)
		return CHAT_ERR_ALREADY_STARTED;

	size_t colon = addr.rfind(':');
	if (colon == std::string_view::npos || colon == 0 || colon + 1 >= addr.size())
		return CHAT_ERR_NO_ADDR;
	std::string host(addr.substr(0, colon));
	std::string service(addr.substr(colon + 1));

	struct addrinfo hints;
	memset(&hints, 0, sizeof(hints));
	hints.ai_family = AF_INET;
	hints.ai_socktype = SOCK_STREAM;
	hints.ai_protocol = IPPROTO_TCP;

	struct addrinfo *res = nullptr;
	int rc = getaddrinfo(host.c_str(), service.c_str(), &hints, &res);
	if (rc != 0)
		return CHAT_ERR_NO_ADDR;

	int sock = -1;
	int err = CHAT_ERR_SYS;
	for (struct addrinfo *it = res; it != nullptr; it = it->ai_next) {
		sock = socket(it->ai_family, it->ai_socktype, it->ai_protocol);
		if (sock < 0)
			continue;
		if (connect(sock, it->ai_addr, it->ai_addrlen) == 0) {
			err = 0;
			break;
		}
		close(sock);
		sock = -1;
	}
	freeaddrinfo(res);
	if (err != 0 || sock < 0)
		return CHAT_ERR_SYS;
	if (socket_make_non_blocking(sock) != 0) {
		close(sock);
		return CHAT_ERR_SYS;
	}
	client->socket = sock;
#if NEED_AUTHOR
	append_frame(client->out_buffer, client->name, std::string_view());
	rc = socket_flush(client->socket, client->out_buffer, client->out_offset);
	if (rc != 0)
		return rc;
#endif
	return 0;
}

struct chat_message *
chat_client_pop_next(struct chat_client *client)
{
	if (client->messages.empty())
		return nullptr;
	chat_message *msg = client->messages.front();
	client->messages.pop_front();
	return msg;
}

int
chat_client_update(struct chat_client *client, double timeout)
{
	if (client->socket < 0)
		return CHAT_ERR_NOT_STARTED;
	int timeout_ms = -1;
	if (timeout >= 0)
		timeout_ms = (int)(timeout * 1000.0 + 0.999999);
	struct pollfd fd;
	memset(&fd, 0, sizeof(fd));
	fd.fd = client->socket;
	fd.events = chat_events_to_poll_events(chat_client_get_events(client));
	int rc = poll(&fd, 1, timeout_ms);
	if (rc < 0)
		return CHAT_ERR_SYS;
	if (rc == 0)
		return CHAT_ERR_TIMEOUT;

	if ((fd.revents & (POLLERR | POLLHUP | POLLNVAL)) != 0)
		return CHAT_ERR_SYS;
	if ((fd.revents & POLLIN) != 0) {
		rc = socket_read_messages(client);
		if (rc != 0)
			return rc;
	}
	if ((fd.revents & POLLOUT) != 0 || client->out_offset < client->out_buffer.size()) {
		rc = socket_flush(client->socket, client->out_buffer, client->out_offset);
		if (rc != 0)
			return rc;
	}
	return 0;
}

int
chat_client_get_descriptor(const struct chat_client *client)
{
	return client->socket;
}

int
chat_client_get_events(const struct chat_client *client)
{
	if (client->socket < 0)
		return 0;
	int events = CHAT_EVENT_INPUT;
	if (client->out_offset < client->out_buffer.size())
		events |= CHAT_EVENT_OUTPUT;
	return events;
}

int
chat_client_feed(struct chat_client *client, const char *msg, uint32_t msg_size)
{
	if (client->socket < 0)
		return CHAT_ERR_NOT_STARTED;
	client->feed_buffer.append(msg, msg_size);
	feed_text(client->feed_buffer, client->out_buffer);
	int rc = socket_flush(client->socket, client->out_buffer, client->out_offset);
	if (rc != 0)
		return rc;
	return 0;
}
