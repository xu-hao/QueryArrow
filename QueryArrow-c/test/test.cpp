#include <stdio.h>
#include "Plugin_stub.h"

// stack ghc -- --make -I.stack-work/dist/x86_64-linux/Cabal-1.24.0.0/build/Client/ -I/home/xuh/.stack/programs/x86_64-linux/ghc-8.0.1/lib/ghc-8.0.1/include/ test/test.cpp -package QueryArrow-c -no-hs-main

int main(int argc, char **argv) {
  void *ptr;
  hs_init(&argc, &argv);
  hs_setup();
  printf("connecting db\n");
  hs_connect(&ptr);
  int status = 0;
  char zone[1024];
  zone[0] = '\0';
  status = hs_get_local_zone(ptr, zone, 1024);
  printf("local zone = %s, status = %d\n", zone, status);
  long rescid = 0;
  char rn[] = "demoResc";
  status = hs_get_int_resc_id_by_name(ptr, rn, &rescid);
  printf("resc id = %ld, status = %d\n", rescid, status);
  status = hs_get_int_resc_id_by_name(ptr, (void *) "notAResc", &rescid);
  printf("resc id = %ld, status = %d\n", rescid, status);

  printf("disconnecting db\n");
  hs_disconnect(ptr);
  hs_exit();
  return 0;
}
