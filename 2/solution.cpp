#include "parser.h"

#include <assert.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <string>
#include <vector>

struct pipeline {
	std::vector<command> commands;
};

struct exec_result {
	int status = 0;
	bool should_exit = false;
};

static int
status_from_wait(int wstatus)
{
	if (WIFEXITED(wstatus))
		return WEXITSTATUS(wstatus);
	if (WIFSIGNALED(wstatus))
		return 128 + WTERMSIG(wstatus);
	return 1;
}

static int
parse_exit_code(const command &cmd)
{
	if (cmd.args.empty())
		return 0;
	const std::string &arg = cmd.args[0];
	char *end = nullptr;
	long code = strtol(arg.c_str(), &end, 10);
	if (end == nullptr || *end != '\0')
		return 255;
	if (code < 0)
		return 255;
	return static_cast<int>(code % 256);
}

static bool
is_builtin_cd(const command &cmd)
{
	return cmd.exe == "cd";
}

static bool
is_builtin_exit(const command &cmd)
{
	return cmd.exe == "exit";
}

static int
run_builtin_child(const command &cmd)
{
	if (is_builtin_cd(cmd)) {
		const char *path = nullptr;
		if (cmd.args.empty())
			path = getenv("HOME");
		else
			path = cmd.args[0].c_str();
		if (path == nullptr)
			path = "";
		if (chdir(path) != 0) {
			perror("cd");
			return 1;
		}
		return 0;
	}
	if (is_builtin_exit(cmd))
		return parse_exit_code(cmd);
	return 127;
}

static std::vector<char *>
make_argv(const command &cmd)
{
	std::vector<char *> argv;
	argv.reserve(cmd.args.size() + 2);
	argv.push_back(const_cast<char *>(cmd.exe.c_str()));
	for (const std::string &arg : cmd.args)
		argv.push_back(const_cast<char *>(arg.c_str()));
	argv.push_back(nullptr);
	return argv;
}

static int
open_output_file(enum output_type out_type, const std::string &out_file)
{
	if (out_type == OUTPUT_TYPE_STDOUT)
		return -1;
	int flags = O_CREAT | O_WRONLY;
	if (out_type == OUTPUT_TYPE_FILE_NEW)
		flags |= O_TRUNC;
	else
		flags |= O_APPEND;
	return open(out_file.c_str(), flags, 0644);
}

static int
run_pipeline(const pipeline &pl, bool apply_redir, enum output_type out_type,
	     const std::string &out_file)
{
	size_t n = pl.commands.size();
	assert(n > 0);

	std::vector<std::array<int, 2>> pipes;
	if (n > 1)
		pipes.resize(n - 1);
	for (size_t i = 0; i + 1 < n; ++i) {
		if (pipe(pipes[i].data()) != 0) {
			perror("pipe");
			return 1;
		}
	}

	std::vector<pid_t> pids;
	pids.reserve(n);
	int last_status = 0;
	for (size_t i = 0; i < n; ++i) {
		pid_t pid = fork();
		if (pid < 0) {
			perror("fork");
			return 1;
		}
		if (pid == 0) {
			if (i > 0)
				dup2(pipes[i - 1][0], STDIN_FILENO);
			if (i + 1 < n)
				dup2(pipes[i][1], STDOUT_FILENO);
			if (apply_redir && i + 1 == n) {
				int fd = open_output_file(out_type, out_file);
				if (fd < 0) {
					perror("open");
					_exit(1);
				}
				dup2(fd, STDOUT_FILENO);
				close(fd);
			}
			for (size_t j = 0; j + 1 < n; ++j) {
				close(pipes[j][0]);
				close(pipes[j][1]);
			}
			const command &cmd = pl.commands[i];
			if (is_builtin_cd(cmd) || is_builtin_exit(cmd)) {
				_exit(run_builtin_child(cmd));
			}
			std::vector<char *> argv = make_argv(cmd);
			execvp(argv[0], argv.data());
			perror(argv[0]);
			_exit(127);
		}
		pids.push_back(pid);
	}

	for (size_t i = 0; i + 1 < n; ++i) {
		close(pipes[i][0]);
		close(pipes[i][1]);
	}
	for (size_t i = 0; i < pids.size(); ++i) {
		int wstatus = 0;
		if (waitpid(pids[i], &wstatus, 0) < 0)
			continue;
		if (i + 1 == pids.size())
			last_status = status_from_wait(wstatus);
	}
	return last_status;
}

static exec_result
execute_line_foreground(const struct command_line *line)
{
	exec_result res;
	std::vector<pipeline> pipelines;
	std::vector<expr_type> ops;
	pipeline current;
	for (const expr &e : line->exprs) {
		if (e.type == EXPR_TYPE_COMMAND) {
			current.commands.push_back(*e.cmd);
		} else if (e.type == EXPR_TYPE_PIPE) {
			continue;
		} else if (e.type == EXPR_TYPE_AND || e.type == EXPR_TYPE_OR) {
			pipelines.push_back(std::move(current));
			current = pipeline();
			ops.push_back(e.type);
		}
	}
	if (!current.commands.empty())
		pipelines.push_back(std::move(current));

	for (size_t i = 0; i < pipelines.size(); ++i) {
		bool run = true;
		if (i > 0) {
			expr_type op = ops[i - 1];
			if (op == EXPR_TYPE_AND && res.status != 0)
				run = false;
			if (op == EXPR_TYPE_OR && res.status == 0)
				run = false;
		}
		if (!run)
			continue;

		const pipeline &pl = pipelines[i];
		bool is_last_pipeline = (i + 1 == pipelines.size());
		bool apply_redir = is_last_pipeline && line->out_type != OUTPUT_TYPE_STDOUT;

		if (pl.commands.size() == 1) {
			const command &cmd = pl.commands[0];
			bool can_exit = line->out_type == OUTPUT_TYPE_STDOUT;
			if (is_builtin_exit(cmd) && can_exit) {
				res.status = parse_exit_code(cmd);
				res.should_exit = true;
				return res;
			}
			if (is_builtin_cd(cmd)) {
				const char *path = nullptr;
				if (cmd.args.empty())
					path = getenv("HOME");
				else
					path = cmd.args[0].c_str();
				if (path == nullptr)
					path = "";
				if (chdir(path) != 0) {
					perror("cd");
					res.status = 1;
				} else {
					res.status = 0;
				}
				continue;
			}
		}

		res.status = run_pipeline(pl, apply_redir, line->out_type, line->out_file);
	}
	return res;
}

static void
reap_zombies()
{
	while (true) {
		int wstatus = 0;
		pid_t pid = waitpid(-1, &wstatus, WNOHANG);
		if (pid <= 0)
			break;
	}
}

static exec_result
execute_command_line(const struct command_line *line)
{
	assert(line != NULL);
	if (!line->is_background)
		return execute_line_foreground(line);

	pid_t pid = fork();
	if (pid < 0) {
		perror("fork");
		exec_result res;
		res.status = 1;
		return res;
	}
	if (pid == 0) {
		exec_result child_res = execute_line_foreground(line);
		_exit(child_res.status);
	}
	exec_result res;
	res.status = 0;
	return res;
}

int
main(void)
{
	const size_t buf_size = 1024;
	char buf[buf_size];
	int rc;
	int last_status = 0;
	struct parser *p = parser_new();
	while ((rc = read(STDIN_FILENO, buf, buf_size)) > 0) {
		parser_feed(p, buf, rc);
		struct command_line *line = NULL;
		while (true) {
			enum parser_error err = parser_pop_next(p, &line);
			if (err == PARSER_ERR_NONE && line == NULL)
				break;
			if (err != PARSER_ERR_NONE) {
				printf("Error: %d\n", (int)err);
				continue;
			}
			exec_result res = execute_command_line(line);
			delete line;
			last_status = res.status;
			reap_zombies();
			if (res.should_exit) {
				parser_delete(p);
				return last_status;
			}
		}
		reap_zombies();
	}
	parser_delete(p);
	reap_zombies();
	return last_status;
}
