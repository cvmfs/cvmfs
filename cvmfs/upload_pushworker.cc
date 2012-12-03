#include "upload_pushworker.h"

using namespace upload;

AbstractPushWorker::Context* AbstractPushWorker::GenerateContext(
                               const std::string &spooler_description) {
  return new Context();
}


AbstractPushWorker::AbstractPushWorker(Context* context) {

}


AbstractPushWorker::~AbstractPushWorker() {

}


bool AbstractPushWorker::Initialize() {

  return false;
}


bool AbstractPushWorker::IsReady() const {

  return false;
}
