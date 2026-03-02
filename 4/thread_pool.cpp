#include "thread_pool.h"

#include <cmath>
#include <ctime>
#include <deque>
#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <vector>

struct thread_task {
	thread_task_f function;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	struct thread_pool *pool = nullptr;
	bool in_pool = false;
	bool is_running = false;
	bool is_finished = false;
	bool is_detached = false;
};

struct thread_pool {
	std::vector<pthread_t> threads;
	std::deque<thread_task *> tasks;
	pthread_mutex_t mutex;
	pthread_cond_t cond;
	int max_threads = 0;
	int idle_threads = 0;
	size_t task_count = 0;
	bool stop = false;
};

static void
thread_task_destroy_object(struct thread_task *task)
{
	pthread_cond_destroy(&task->cond);
	pthread_mutex_destroy(&task->mutex);
	delete task;
}

static void *
thread_pool_worker_f(void *arg)
{
	auto *pool = (thread_pool *)arg;
	while (true) {
		pthread_mutex_lock(&pool->mutex);
		while (pool->tasks.empty() && !pool->stop) {
			++pool->idle_threads;
			pthread_cond_wait(&pool->cond, &pool->mutex);
			--pool->idle_threads;
		}
		if (pool->stop && pool->tasks.empty()) {
			pthread_mutex_unlock(&pool->mutex);
			return nullptr;
		}
		thread_task *task = pool->tasks.front();
		pool->tasks.pop_front();
		pthread_mutex_unlock(&pool->mutex);

		pthread_mutex_lock(&task->mutex);
		task->is_running = true;
		pthread_mutex_unlock(&task->mutex);

		task->function();

		bool need_delete = false;
		pthread_mutex_lock(&task->mutex);
		task->is_running = false;
		task->is_finished = true;
		pthread_cond_broadcast(&task->cond);
		if (task->is_detached) {
			task->in_pool = false;
			task->pool = nullptr;
			need_delete = true;
		}
		pthread_mutex_unlock(&task->mutex);

		if (need_delete) {
			pthread_mutex_lock(&pool->mutex);
			if (pool->task_count > 0)
				--pool->task_count;
			pthread_mutex_unlock(&pool->mutex);
			thread_task_destroy_object(task);
		}
	}
}

static void
thread_pool_start_worker(struct thread_pool *pool)
{
	pthread_t thread;
	int rc = pthread_create(&thread, nullptr, thread_pool_worker_f, pool);
	if (rc != 0)
		abort();
	pool->threads.push_back(thread);
}

static void
timespec_from_timeout(double timeout, struct timespec *ts)
{
	clock_gettime(CLOCK_REALTIME, ts);
	if (timeout <= 0) {
		return;
	}
	if (!std::isfinite(timeout) || timeout > 1000000000.0) {
		ts->tv_sec += 1000000000L;
		return;
	}
	double sec_part = floor(timeout);
	double nsec_part = (timeout - sec_part) * 1000000000.0;
	long add_sec = (long)sec_part;
	long add_nsec = (long)nsec_part;
	ts->tv_sec += add_sec;
	ts->tv_nsec += add_nsec;
	if (ts->tv_nsec >= 1000000000L) {
		ts->tv_sec += ts->tv_nsec / 1000000000L;
		ts->tv_nsec %= 1000000000L;
	}
}

int
thread_pool_new(int thread_count, struct thread_pool **pool)
{
	if (thread_count <= 0 || thread_count > TPOOL_MAX_THREADS)
		return TPOOL_ERR_INVALID_ARGUMENT;
	thread_pool *res = new thread_pool();
	pthread_mutex_init(&res->mutex, nullptr);
	pthread_cond_init(&res->cond, nullptr);
	res->max_threads = thread_count;
	*pool = res;
	return 0;
}

int
thread_pool_delete(struct thread_pool *pool)
{
	pthread_mutex_lock(&pool->mutex);
	if (pool->task_count != 0) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_HAS_TASKS;
	}
	pool->stop = true;
	pthread_cond_broadcast(&pool->cond);
	pthread_mutex_unlock(&pool->mutex);

	for (pthread_t thread : pool->threads)
		pthread_join(thread, nullptr);
	pthread_cond_destroy(&pool->cond);
	pthread_mutex_destroy(&pool->mutex);
	delete pool;
	return 0;
}

int
thread_pool_push_task(struct thread_pool *pool, struct thread_task *task)
{
	pthread_mutex_lock(&pool->mutex);
	if (pool->task_count >= TPOOL_MAX_TASKS) {
		pthread_mutex_unlock(&pool->mutex);
		return TPOOL_ERR_TOO_MANY_TASKS;
	}

	pthread_mutex_lock(&task->mutex);
	task->pool = pool;
	task->in_pool = true;
	task->is_running = false;
	task->is_finished = false;
	task->is_detached = false;
	pthread_mutex_unlock(&task->mutex);

	pool->tasks.push_back(task);
	++pool->task_count;
	if (pool->idle_threads == 0 &&
	    (int)pool->threads.size() < pool->max_threads)
		thread_pool_start_worker(pool);
	pthread_cond_signal(&pool->cond);
	pthread_mutex_unlock(&pool->mutex);
	return 0;
}

int
thread_task_new(struct thread_task **task, const thread_task_f &function)
{
	thread_task *res = new thread_task();
	res->function = function;
	pthread_mutex_init(&res->mutex, nullptr);
	pthread_cond_init(&res->cond, nullptr);
	*task = res;
	return 0;
}

bool
thread_task_is_finished(const struct thread_task *task)
{
	pthread_mutex_lock((pthread_mutex_t *)&task->mutex);
	bool res = task->is_finished && !task->in_pool;
	pthread_mutex_unlock((pthread_mutex_t *)&task->mutex);
	return res;
}

bool
thread_task_is_running(const struct thread_task *task)
{
	pthread_mutex_lock((pthread_mutex_t *)&task->mutex);
	bool res = task->is_running;
	pthread_mutex_unlock((pthread_mutex_t *)&task->mutex);
	return res;
}

int
thread_task_join(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	if (!task->in_pool) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	while (!task->is_finished)
		pthread_cond_wait(&task->cond, &task->mutex);
	thread_pool *pool = task->pool;
	task->in_pool = false;
	task->pool = nullptr;
	pthread_mutex_unlock(&task->mutex);

	pthread_mutex_lock(&pool->mutex);
	if (pool->task_count > 0)
		--pool->task_count;
	pthread_mutex_unlock(&pool->mutex);
	return 0;
}

#if NEED_TIMED_JOIN

int
thread_task_timed_join(struct thread_task *task, double timeout)
{
	pthread_mutex_lock(&task->mutex);
	if (!task->in_pool) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	if (!task->is_finished) {
		if (timeout <= 0) {
			pthread_mutex_unlock(&task->mutex);
			return TPOOL_ERR_TIMEOUT;
		}
		struct timespec ts;
		timespec_from_timeout(timeout, &ts);
		while (!task->is_finished) {
			int rc = pthread_cond_timedwait(&task->cond, &task->mutex, &ts);
			if (rc == ETIMEDOUT && !task->is_finished) {
				pthread_mutex_unlock(&task->mutex);
				return TPOOL_ERR_TIMEOUT;
			}
		}
	}
	thread_pool *pool = task->pool;
	task->in_pool = false;
	task->pool = nullptr;
	pthread_mutex_unlock(&task->mutex);

	pthread_mutex_lock(&pool->mutex);
	if (pool->task_count > 0)
		--pool->task_count;
	pthread_mutex_unlock(&pool->mutex);
	return 0;
}

#endif

int
thread_task_delete(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	if (task->in_pool) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_IN_POOL;
	}
	pthread_mutex_unlock(&task->mutex);
	thread_task_destroy_object(task);
	return 0;
}

#if NEED_DETACH

int
thread_task_detach(struct thread_task *task)
{
	pthread_mutex_lock(&task->mutex);
	if (!task->in_pool) {
		pthread_mutex_unlock(&task->mutex);
		return TPOOL_ERR_TASK_NOT_PUSHED;
	}
	task->is_detached = true;
	if (!task->is_finished) {
		pthread_mutex_unlock(&task->mutex);
		return 0;
	}
	thread_pool *pool = task->pool;
	task->in_pool = false;
	task->pool = nullptr;
	pthread_mutex_unlock(&task->mutex);

	pthread_mutex_lock(&pool->mutex);
	if (pool->task_count > 0)
		--pool->task_count;
	pthread_mutex_unlock(&pool->mutex);
	thread_task_destroy_object(task);
	return 0;
}

#endif
