
#include <unistd.h>
#include <string>
#include "history.h"

using namespace std;

int main() {
  string filename = "testhistory.db";
  unlink(filename.c_str());
  history::Database::Create(filename);
  history::Database db(filename, sqlite::kDbOpenReadWrite);

  return 0;
}
