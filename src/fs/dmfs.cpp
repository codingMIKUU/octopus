#include "RPCServer.hpp"
#include <sys/wait.h>  
#include <sys/types.h>
#include <signal.h>
#include <unistd.h>

RPCServer *server;

static volatile sig_atomic_t g_stop = 0;
static volatile sig_atomic_t g_force_exit = 0;

/* Catch ctrl-c and destruct. */
void Stop (int signo) {
    (void)signo;
    g_stop = 1;
}

static void ForceExit(int signo) {
    (void)signo;
    g_force_exit = 1;
    _exit(0);
}
int main() {
    signal(SIGINT, Stop);
    signal(SIGTERM, Stop);
    signal(SIGALRM, ForceExit);
    server = new RPCServer(17);

    while (!g_stop) {
        pause();
    }

	/*
	 * Best-effort graceful shutdown; if it hangs (e.g., waiting on RDMA workers),
	 * SIGALRM will force-exit after a short grace period.
	 */
	alarm(3);
    delete server;
	alarm(0);
    Debug::notifyInfo("DMFS is terminated, Bye.");
    return 0;
}