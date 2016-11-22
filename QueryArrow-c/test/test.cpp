#include <stdio.h>
#include <stdlib.h>
#include "Plugin_stub.h"
#include "PluginGen_stub.h"
#include "GenQuery_stub.h"

// stack ghc -- --make -I.stack-work/dist/x86_64-linux/Cabal-1.24.0.0/build/Client/ test/test.cpp -package QueryArrow-c -no-hs-main

void runGenQuery(void *ptr, const char *qu) {
  char **out = NULL;
  int col = 0;
  int row = 0;
  int status = hs_gen_query(ptr, (void *) qu, &out, &col, &row);
  printf("results: %d %d, status = %d\n", col, row, status);
  for(int i=0;i<row;i++) {
    for(int j=0;j<col;j++) {
      printf("%s,", out[i*col + j]);
    }
    printf("\b\n");
  }

}
int main(int argc, char **argv) {
  void *ptr;
  hs_init(&argc, &argv);
  hs_setup();
  printf("connecting db\n");
  char path[] = "test/tdb-plugin.json";
  hs_connect((void *) path, &ptr);
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
  char **zone_info = NULL;
  int len = 0;
  status = hs_get_all_zone_info(ptr, &zone_info, &len);
  printf("results: %d, status = %d\n", len, status);
  for(int i=0;i<len;i++) {
    printf("%s\n", zone_info[i]);
    free(zone_info[i]);
  }
  free(zone_info);
  char passwords[100 * 4];
  status = hs_get_some_user_password_by_user_zone_and_name(ptr, (void *) "tempZone", (void *) "rods", passwords, 100, 4);
  printf("results: [%s, %s, %s, %s], status = %d\n", passwords, passwords + 100, passwords + 200, passwords + 300, status);
  runGenQuery(ptr, "select COLL_ID, COLL_NAME, COLL_OWNER_NAME, COLL_OWNER_ZONE, COLL_CREATE_TIME, COLL_MODIFY_TIME, COLUMN_NAME_NOT_FOUND_510, COLUMN_NAME_NOT_FOUND_511, COLUMN_NAME_NOT_FOUND_512 where COLL_NAME parent_of '/tempZone/home/rods' and COLUMN_NAME_NOT_FOUND_510 like '_%'");
  runGenQuery(ptr, "select DATA_ID, DATA_SIZE, COLUMN_NAME_NOT_FOUND_421, DATA_REPL_STATUS, DATA_CHECKSUM, DATA_OWNER_NAME, DATA_OWNER_ZONE, DATA_CREATE_TIME, DATA_MODIFY_TIME, DATA_RESC_ID where COLL_NAME ='/tempZone/home' and DATA_NAME ='rods'");
  runGenQuery(ptr, "select COLL_NAME, COLL_OWNER_NAME, COLL_CREATE_TIME, COLL_MODIFY_TIME, COLUMN_NAME_NOT_FOUND_510, COLUMN_NAME_NOT_FOUND_511, COLUMN_NAME_NOT_FOUND_512 where COLL_NAME <>'/' and COLL_PARENT_NAME ='/tempZone/home/rods'");

  long collid;
  char dataid[] = "10050";
  status = hs_get_int_coll_id_if_own_access_by_user_zone_and_name(ptr, (void *) "tempZone", (void *) "rods", dataid, &collid);
  printf("results: %ld, status = %d\n", collid, status);
  printf("disconnecting db\n");
  hs_disconnect(ptr);
  hs_exit();
  return 0;
}
