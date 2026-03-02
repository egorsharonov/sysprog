#include "userfs.h"

#include "rlist.h"

#include <algorithm>
#include <cstring>
#include <stddef.h>
#include <string>
#include <vector>

enum {
	BLOCK_SIZE = 512,
	MAX_FILE_SIZE = 1024 * 1024 * 100,
};

/** Global error code. Set from any function on any error. */
static ufs_error_code ufs_error_code = UFS_ERR_NO_ERR;

struct block {
	/** Block memory. */
	char memory[BLOCK_SIZE];
	/** A link in the block list of the owner-file. */
	rlist in_block_list = RLIST_LINK_INITIALIZER;
};

struct file {
	/**
	 * Doubly-linked intrusive list of file blocks. Intrusiveness of the
	 * list gives you the full control over the lifetime of the items in the
	 * list without having to use double pointers with performance penalty.
	 */
	rlist blocks = RLIST_HEAD_INITIALIZER(blocks);
	/** How many file descriptors are opened on the file. */
	int refs = 0;
	/** File name. */
	std::string name;
	/** A link in the global file list. */
	rlist in_file_list = RLIST_LINK_INITIALIZER;
	size_t size = 0;
	size_t block_count = 0;
	bool is_deleted = false;
};

/**
 * Intrusive list of all files. In this case the intrusiveness of the list also
 * grants the ability to remove items from any position in O(1) complexity
 * without having to know their iterator.
 */
static rlist file_list = RLIST_HEAD_INITIALIZER(file_list);

struct filedesc {
	file *atfile;
	size_t position = 0;
#if NEED_OPEN_FLAGS
	int access_flags = UFS_READ_WRITE;
#endif
};

/**
 * An array of file descriptors. When a file descriptor is
 * created, its pointer drops here. When a file descriptor is
 * closed, its place in this array is set to NULL and can be
 * taken by next ufs_open() call.
 */
static std::vector<filedesc*> file_descriptors;

static file *
file_find(const char *filename)
{
	file *it;
	rlist_foreach_entry(it, &file_list, in_file_list) {
		if (it->name == filename)
			return it;
	}
	return nullptr;
}

static block *
file_get_block(file *f, size_t idx)
{
	if (idx >= f->block_count)
		return nullptr;
	rlist *pos = rlist_first(&f->blocks);
	while (idx > 0) {
		pos = rlist_next(pos);
		--idx;
	}
	return rlist_entry(pos, block, in_block_list);
}

static void
file_drop_blocks(file *f)
{
	block *it, *tmp;
	rlist_foreach_entry_safe(it, &f->blocks, in_block_list, tmp) {
		rlist_del_entry(it, in_block_list);
		delete it;
	}
	f->block_count = 0;
	f->size = 0;
}

static void
file_delete_object(file *f)
{
	file_drop_blocks(f);
	delete f;
}

static int
file_ensure_blocks(file *f, size_t count)
{
	while (f->block_count < count) {
		block *b = new block();
		memset(b->memory, 0, sizeof(b->memory));
		rlist_add_tail_entry(&f->blocks, b, in_block_list);
		++f->block_count;
	}
	return 0;
}

static void
file_truncate_blocks(file *f, size_t count)
{
	while (f->block_count > count) {
		block *b = rlist_last_entry(&f->blocks, block, in_block_list);
		rlist_del_entry(b, in_block_list);
		delete b;
		--f->block_count;
	}
}

static void
file_zero_range(file *f, size_t from, size_t size)
{
	if (size == 0)
		return;
	size_t block_idx = from / BLOCK_SIZE;
	size_t block_off = from % BLOCK_SIZE;
	block *cur = file_get_block(f, block_idx);
	while (size > 0 && cur != nullptr) {
		size_t chunk = std::min(size, BLOCK_SIZE - block_off);
		memset(cur->memory + block_off, 0, chunk);
		size -= chunk;
		block_off = 0;
		if (size > 0) {
			rlist *next = rlist_next(&cur->in_block_list);
			if (next == &f->blocks)
				break;
			cur = rlist_entry(next, block, in_block_list);
		}
	}
}

static bool
fd_is_valid(int fd)
{
	return fd >= 0 && (size_t)fd < file_descriptors.size() &&
		file_descriptors[fd] != nullptr;
}

static void
file_unref(file *f)
{
	--f->refs;
	if (f->refs == 0 && f->is_deleted)
		file_delete_object(f);
}

enum ufs_error_code
ufs_errno()
{
	return ufs_error_code;
}

int
ufs_open(const char *filename, int flags)
{
	if (filename == nullptr) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	file *f = file_find(filename);
	if (f == nullptr) {
		if ((flags & UFS_CREATE) == 0) {
			ufs_error_code = UFS_ERR_NO_FILE;
			return -1;
		}
		f = new file();
		f->name = filename;
		rlist_add_tail_entry(&file_list, f, in_file_list);
	}
	filedesc *desc = new filedesc();
	desc->atfile = f;
#if NEED_OPEN_FLAGS
	int mode = flags & UFS_READ_WRITE;
	if (mode == 0)
		mode = UFS_READ_WRITE;
	desc->access_flags = mode;
#else
	(void)flags;
#endif

	size_t idx = 0;
	while (idx < file_descriptors.size() && file_descriptors[idx] != nullptr)
		++idx;
	if (idx == file_descriptors.size())
		file_descriptors.push_back(desc);
	else
		file_descriptors[idx] = desc;
	++f->refs;
	ufs_error_code = UFS_ERR_NO_ERR;
	return (int)idx;
}

ssize_t
ufs_write(int fd, const char *buf, size_t size)
{
	if (!fd_is_valid(fd)) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	filedesc *desc = file_descriptors[fd];
#if NEED_OPEN_FLAGS
	if ((desc->access_flags & UFS_WRITE_ONLY) == 0) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}
#endif
	if (size == 0) {
		ufs_error_code = UFS_ERR_NO_ERR;
		return 0;
	}
	file *f = desc->atfile;
	if (desc->position > MAX_FILE_SIZE || size > MAX_FILE_SIZE - desc->position) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return -1;
	}
	size_t end_pos = desc->position + size;
	size_t need_blocks = (end_pos + BLOCK_SIZE - 1) / BLOCK_SIZE;
	if (file_ensure_blocks(f, need_blocks) != 0) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return -1;
	}
	size_t block_idx = desc->position / BLOCK_SIZE;
	size_t block_off = desc->position % BLOCK_SIZE;
	block *cur = file_get_block(f, block_idx);
	size_t left = size;
	const char *src = buf;
	while (left > 0 && cur != nullptr) {
		size_t chunk = std::min(left, BLOCK_SIZE - block_off);
		memcpy(cur->memory + block_off, src, chunk);
		left -= chunk;
		src += chunk;
		block_off = 0;
		if (left > 0) {
			rlist *next = rlist_next(&cur->in_block_list);
			if (next == &f->blocks)
				cur = nullptr;
			else
				cur = rlist_entry(next, block, in_block_list);
		}
	}
	desc->position = end_pos;
	if (f->size < end_pos)
		f->size = end_pos;
	ufs_error_code = UFS_ERR_NO_ERR;
	return (ssize_t)size;
}

ssize_t
ufs_read(int fd, char *buf, size_t size)
{
	if (!fd_is_valid(fd)) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	filedesc *desc = file_descriptors[fd];
#if NEED_OPEN_FLAGS
	if ((desc->access_flags & UFS_READ_ONLY) == 0) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}
#endif
	file *f = desc->atfile;
	if (desc->position >= f->size || size == 0) {
		ufs_error_code = UFS_ERR_NO_ERR;
		return 0;
	}
	size_t readable = f->size - desc->position;
	size_t to_read = std::min(readable, size);
	size_t block_idx = desc->position / BLOCK_SIZE;
	size_t block_off = desc->position % BLOCK_SIZE;
	block *cur = file_get_block(f, block_idx);
	size_t left = to_read;
	char *dst = buf;
	while (left > 0 && cur != nullptr) {
		size_t chunk = std::min(left, BLOCK_SIZE - block_off);
		memcpy(dst, cur->memory + block_off, chunk);
		left -= chunk;
		dst += chunk;
		block_off = 0;
		if (left > 0) {
			rlist *next = rlist_next(&cur->in_block_list);
			if (next == &f->blocks)
				cur = nullptr;
			else
				cur = rlist_entry(next, block, in_block_list);
		}
	}
	desc->position += to_read;
	ufs_error_code = UFS_ERR_NO_ERR;
	return (ssize_t)to_read;
}

int
ufs_close(int fd)
{
	if (!fd_is_valid(fd)) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	filedesc *desc = file_descriptors[fd];
	file_descriptors[fd] = nullptr;
	file_unref(desc->atfile);
	delete desc;
	ufs_error_code = UFS_ERR_NO_ERR;
	return 0;
}

int
ufs_delete(const char *filename)
{
	if (filename == nullptr) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	file *f = file_find(filename);
	if (f == nullptr) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	rlist_del_entry(f, in_file_list);
	f->is_deleted = true;
	if (f->refs == 0)
		file_delete_object(f);
	ufs_error_code = UFS_ERR_NO_ERR;
	return 0;
}

#if NEED_RESIZE

int
ufs_resize(int fd, size_t new_size)
{
	if (!fd_is_valid(fd)) {
		ufs_error_code = UFS_ERR_NO_FILE;
		return -1;
	}
	filedesc *desc = file_descriptors[fd];
#if NEED_OPEN_FLAGS
	if ((desc->access_flags & UFS_WRITE_ONLY) == 0) {
		ufs_error_code = UFS_ERR_NO_PERMISSION;
		return -1;
	}
#endif
	if (new_size > MAX_FILE_SIZE) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return -1;
	}
	file *f = desc->atfile;
	size_t old_size = f->size;
	size_t need_blocks = (new_size + BLOCK_SIZE - 1) / BLOCK_SIZE;
	if (new_size == 0)
		need_blocks = 0;
	if (need_blocks > f->block_count && file_ensure_blocks(f, need_blocks) != 0) {
		ufs_error_code = UFS_ERR_NO_MEM;
		return -1;
	}
	if (new_size > old_size)
		file_zero_range(f, old_size, new_size - old_size);
	if (need_blocks < f->block_count)
		file_truncate_blocks(f, need_blocks);
	if (new_size > 0 && (new_size % BLOCK_SIZE) != 0 && need_blocks > 0) {
		block *last = rlist_last_entry(&f->blocks, block, in_block_list);
		size_t from = new_size % BLOCK_SIZE;
		memset(last->memory + from, 0, BLOCK_SIZE - from);
	}
	f->size = new_size;
	for (filedesc *it : file_descriptors) {
		if (it != nullptr && it->atfile == f && it->position > new_size)
			it->position = new_size;
	}
	ufs_error_code = UFS_ERR_NO_ERR;
	return 0;
}

#endif

void
ufs_destroy(void)
{
	/*
	 * The file_descriptors array is likely to leak even if
	 * you resize it to zero or call clear(). This is because
	 * the vector keeps memory reserved in case more elements
	 * would be added.
	 *
	 * The recommended way of freeing the memory is to swap()
	 * the vector with a temporary empty vector.
	 */
	for (filedesc *desc : file_descriptors) {
		if (desc == nullptr)
			continue;
		file_unref(desc->atfile);
		delete desc;
	}
	std::vector<filedesc*> empty_fds;
	file_descriptors.swap(empty_fds);

	file *it, *tmp;
	rlist_foreach_entry_safe(it, &file_list, in_file_list, tmp) {
		rlist_del_entry(it, in_file_list);
		file_delete_object(it);
	}
}
