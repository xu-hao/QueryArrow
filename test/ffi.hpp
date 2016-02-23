#include <string.h>

typedef void db_handle_t;
typedef int state_t;
typedef int succ_func_t(state_t *, int, char **);
typedef void err_func_t(state_t *, char **);
int start(std::string ps, std::string verifier_info, db_handle_t *&db_handle);
void exec_query(db_handle_t *db_handle, std::string query, state_t &state, int n, succ_func_t *succ, err_func_t *err);
void stop(db_handle_t *&);


