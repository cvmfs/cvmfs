#include "upload_pushworker.h"

using namespace upload;


AbstractPushWorker::AbstractPushWorker() {

}


AbstractPushWorker::~AbstractPushWorker() {

}


bool AbstractPushWorker::Initialize() {

  return true;
}


bool AbstractPushWorker::IsReady() const {

  return true;
}
