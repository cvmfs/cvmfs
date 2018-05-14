#include <sys/inotify.h>

int main() {
    int ret = inotify_init1(IN_NONBLOCK);
    return 0;
}