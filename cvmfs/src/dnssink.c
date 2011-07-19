

#include <stdio.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#define MSGBUFSIZE 512

int main() {
   int fd_recv;
   struct sockaddr_in addr_this;
   int retval;
   
   fd_recv = socket(PF_INET, SOCK_DGRAM, IPPROTO_UDP);
   if (fd_recv < 0)
   {
      printf("Failed to setup socket\n");
      return 1;
   }
   
   memset(&addr_this, 0, sizeof(addr_this));
   addr_this.sin_family = AF_INET;
   addr_this.sin_addr.s_addr = inet_addr("127.0.0.1");
   addr_this.sin_port = htons(53);   
   
   retval = bind(fd_recv, (struct sockaddr *)&addr_this, sizeof(addr_this));
   if (retval != 0) {
      printf("Failed to bind socket\n");
      return 1;
   }
   
   char msgbuf[MSGBUFSIZE];
   struct sockaddr_in addr_sender;
   socklen_t addr_sender_len = sizeof(addr_sender);
   int nbytes;
   printf("Starting sink\n");
   while (1) {
      nbytes = recvfrom(fd_recv, msgbuf, MSGBUFSIZE-1, 0, (struct sockaddr *)&addr_sender, &addr_sender_len);
      if (nbytes < 0)
         break;
         
      
      if (nbytes > 0)
      {
         msgbuf[nbytes] = 0;
         printf("Received:\n%s\n", msgbuf);
      } else {
         printf("0 Bytes received");
      }
   }
   
   return 0;
}
