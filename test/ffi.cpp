#include <fstream>
#include "ffi/FFI_stub.h"
#include <string.h>
#include <stdlib.h>

typedef void db_handle_t;
typedef int state_t;
struct handler_t {
    std::function<int(int *, int, char **)> succ_func;
    std::function<void(int *, char **)> err_func;
};
typedef int succ_func_t(handler_t *, state_t *, int, char **);
typedef void err_func_t(handler_t *, state_t *, char **);
int start(std::string ps, std::string verifier_info, db_handle_t *&db_handle);
void exec_query(db_handle_t *db_handle, std::string query, handler_t &handler, state_t &state, int n, succ_func_t *succ, err_func_t *err);
void stop(db_handle_t *&);

void err_handler(handler_t *handler, int *state, char **res) {
    handler -> err_func(state, res);
}

int succ_handler(handler_t *handler, int *state, int n, char **res) {
    handler->succ_func(state, n, res);
}

void err_func(int *state, char **res) {

	while (*res!=NULL) {
		printf("Error:: %s\n", *res);
		res++;
	}

}
int succ_func(int *state, int n, char **res)

 {
	printf("batch begin\n");
	while (*res!=NULL) {
			for(int i= 0;i<n;i++) {
				printf("%s ", *res);
				res++;
			}

			printf("\n");
	}
	printf("batch end\n");
	return n;

}

void exec_query(handler_t &handler, db_handle_t *db_handle, std::string query, state_t &state, int n) {
	char *qua = const_cast<char*>(query.c_str());
	run3(db_handle, qua, &handler, &state, n, reinterpret_cast<HsFunPtr>(succ_func), reinterpret_cast<HsFunPtr>(err_func));

}

int start(std::string ps, std::string verifier_info, db_handle_t *&db_handle) {
	// initilize ghc runtime
	int argc = 1;
	char *argv0[1] = {const_cast<char*>("dummy")};
	char **argv = argv0;
	hs_init(&argc, &argv);
	char *psa = const_cast<char*>(ps.c_str());
	char *verifier_infoa = const_cast<char*>(verifier_info.c_str());
	void **res = reinterpret_cast<void **>(init3(psa, verifier_infoa));
	char *status = reinterpret_cast<char *>(*res);
	res++;

	if(strcmp(status, "Error") == 0) {
		while (*res!=NULL) {
			printf("Error Initializing DB:: %s\n", reinterpret_cast<char *>(*res));
			res++;
		}
		return -1;
	} else {
		printf("Status:: %s\n", status);
		db_handle = *res;
		return 0;
	}

}

void stop(db_handle_t *&db_handle) {
	// exit ghc runtime
	hs_exit();

}
int main(int argc, char **argv) {

	std::ifstream t(argv[1]);
	std::string ps((std::istreambuf_iterator<char>(t)),
                 std::istreambuf_iterator<char>());

 	std::ifstream s(argv[2]);
 	std::string verifier_info((std::istreambuf_iterator<char>(s)),
                  std::istreambuf_iterator<char>());
	db_handle_t *db_handle;
	if(start(ps, verifier_info, db_handle) == 0) {
		std::string query(argv[3]);
		state_t state = atoi(argv[4]);
                
        handler_t handler = {succ_handler, err_handler};
        
	for(int i=0;i<(argc > 6?atoi(argv[6]):1);i++) {
                exec_query(db_handle, query, handler, state, argc > 5 ? atoi(argv[5]) : 1000);
	}
	stop(db_handle);
}


	return 0;
}
