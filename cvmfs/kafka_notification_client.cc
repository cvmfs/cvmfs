/**
 * This file is part of the CernVM File System.
 */

#include "kafka_notification_client.h"

#include <string>
#include <vector>

#include "logging.h"
#include "manifest.h"
#include "manifest_fetch.h"
#include "notify/messages.h"
#include "notify/subscriber_sse.h"
#include "notify/subscriber_supervisor.h"
#include "signature.h"
#include "supervisor.h"
#include "util/posix.h"
#include "librdkafka/rdkafka.h"

#define ERRMSG "Could not start kafka notification client. (line %d) CVMFS will not get publication notifications: %s", __LINE__

int KafkaNotificationClient::Consume(const std::string& repo_name,
                                             const std::string& msg_text) {

    notify::msg::Activity msg;
    if (!msg.FromJSONStringKafka(msg_text)) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "KafkaNotificationClient - could not decode JSON message.");
      return notify::Subscriber::kError;
    }

    if (repo_name != msg.repository_) {
      // message is about a different repository
      LogCvmfs(kLogCvmfs, kLogDebug, "KafkaNotificationClient - message is about a different repo");
      return notify::Subscriber::kNothingToDo;
    }

    int current_revision = remounter_->GetRevision();
    LogCvmfs(kLogCvmfs, kLogDebug,
             "KafkaNotificationClient - repository %s is now advertised as revision %lu. Currently using revision %lu",
             repo_name.c_str(), msg.version_, current_revision );


    if( current_revision >= msg.version_ ) {
      // already uptodate, surprisingly
      LogCvmfs(kLogCvmfs, kLogDebug, "KafkaNotificationClient - message is about a revision we already have %d vs %d", msg.version_, current_revision );
      return notify::Subscriber::kNothingToDo;
    }

    FuseRemounter::Status status = remounter_->CheckSynchronously();

    switch (status) {
      case FuseRemounter::kStatusUp2Date: // the success case
        LogCvmfs(kLogCvmfs,kLogDebug,
                 "KafkaNotificationClient - catalog up to date on revision %d", remounter_->GetRevision());
        break;
      case FuseRemounter::kStatusFailGeneral:
        LogCvmfs(kLogCvmfs, kLogDebug, "KafkaNotificationClient - remount failed");
        break;
      case FuseRemounter::kStatusFailNoSpace:
        LogCvmfs(kLogCvmfs,kLogDebug,
                 "KafkaNotificationClient - remount failed (no space)");
        break;
        case FuseRemounter::kStatusMaintenance:
        LogCvmfs(kLogCvmfs,kLogDebug,
                 "KafkaNotificationClient - in maintenance mode");
        break;
      default:
        LogCvmfs(kLogCvmfs,kLogDebug, "KafkaNotificationClient - internal error");
    }
    return notify::Subscriber::kContinue;
  }


KafkaNotificationClient::KafkaNotificationClient(const std::string& broker,
                                       const std::string& username,
                                       const std::string& password,
                                       const std::string& topic,
                                       const std::string& repo_name,
                                       FuseRemounter* remounter,
                                       download::DownloadManager* dl_mgr,
                                       signature::SignatureManager* sig_mgr)
    : broker_(broker),
      username_(username),
      password_(password),
      topic_(topic),
      repo_name_(repo_name),
      remounter_(remounter),
      dl_mgr_(dl_mgr),
      sig_mgr_(sig_mgr),
      thread_(),
      shutdown_(false),
      spawned_(false) {}

KafkaNotificationClient::~KafkaNotificationClient() {
  if (subscriber_.IsValid()) {
    subscriber_->Unsubscribe();
  }
  if (spawned_) {
    shutdown_=true;
    pthread_join(thread_, NULL);
    spawned_ = false;
    shutdown_=false;
  }
}

void KafkaNotificationClient::Spawn() {
  if (!spawned_) {
    if (pthread_create(&thread_, NULL, KafkaNotificationClient::Run, this)) {
      LogCvmfs(kLogCvmfs, kLogDebug,
               "KafkaNotificationClient - Could not start background thread");
      return;
    } else {
      LogCvmfs(kLogCvmfs, kLogDebug, "KafkaNotificationClient: spawned background thread" );
    }
    spawned_ = true;
  } else {
    LogCvmfs(kLogCvmfs, kLogDebug, "KafkaNotificationClient - not spawning -- already spawned" );
  }
}

void* KafkaNotificationClient::Run(void* data) {
  KafkaNotificationClient* cl = static_cast<KafkaNotificationClient*>(data);

  LogCvmfs( kLogCvmfs, kLogDebug, "KafkaNotificationClient - Starting listening thread for [%s]", cl->repo_name_.c_str());
  LogCvmfs( kLogCvmfs, kLogDebug, "KafkaNotificationClient - Topic is [%s]", cl->topic_.c_str());

  rd_kafka_t *rk;
  rd_kafka_topic_t *rkt;
  rd_kafka_conf_t *conf;
  char errstr[512];

  conf = rd_kafka_conf_new();

  if(rd_kafka_conf_set(conf, "bootstrap.servers", cl->broker_.c_str(), errstr, sizeof(errstr))!= RD_KAFKA_CONF_OK ) {
    LogCvmfs(kLogCvmfs, kLogDebug, ERRMSG, errstr );
    rd_kafka_conf_destroy(conf);
    return NULL;
  }

  if(rd_kafka_conf_set(conf, "security.protocol", "sasl_plaintext", errstr, sizeof(errstr))!= RD_KAFKA_CONF_OK ) {
    LogCvmfs(kLogCvmfs, kLogDebug, ERRMSG, errstr );
    rd_kafka_conf_destroy(conf);
    return NULL;
  }
  if(rd_kafka_conf_set(conf, "sasl.mechanism", "SCRAM-SHA-512", errstr, sizeof(errstr))!= RD_KAFKA_CONF_OK ) {
    LogCvmfs(kLogCvmfs, kLogDebug, ERRMSG, errstr );
    rd_kafka_conf_destroy(conf);
    return NULL;
  }
  if(rd_kafka_conf_set(conf, "sasl.username", cl->username_.c_str(), errstr, sizeof(errstr))!= RD_KAFKA_CONF_OK ) {
    LogCvmfs(kLogCvmfs, kLogDebug, ERRMSG, errstr );
    rd_kafka_conf_destroy(conf);
    return NULL;
  }
  if(rd_kafka_conf_set(conf, "sasl.password", cl->password_.c_str(), errstr, sizeof(errstr))!= RD_KAFKA_CONF_OK ) {
    LogCvmfs(kLogCvmfs, kLogDebug, ERRMSG, errstr );
    rd_kafka_conf_destroy(conf);
    return NULL;
  }
  if(rd_kafka_conf_set(conf, "auto.offset.reset", "end", errstr, sizeof(errstr))!= RD_KAFKA_CONF_OK ) {
    LogCvmfs(kLogCvmfs, kLogDebug, ERRMSG, errstr );
    rd_kafka_conf_destroy(conf);
    return NULL;
  }


  rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));

  if( !rk ) {
    LogCvmfs(kLogCvmfs, kLogDebug , ERRMSG, errstr );
    rd_kafka_conf_destroy(conf);
    return NULL;
  }
  conf=NULL; // now owned by the rd_kafka_t instance 

  rkt = rd_kafka_topic_new( rk, cl->topic_.c_str(), rd_kafka_default_topic_conf_dup(rk));
  if( !rkt ) {
    LogCvmfs(kLogCvmfs, kLogDebug , ERRMSG, errstr );
    return NULL;
  }

  int errx = rd_kafka_consume_start( rkt, 0, RD_KAFKA_OFFSET_END );
  if( errx ) {
    LogCvmfs(kLogCvmfs, kLogDebug , ERRMSG, errstr ); 
    return NULL;
  }

  LogCvmfs( kLogCvmfs, kLogDebug, "KafkaNotificationClient - Entering message loop" );

  while(!cl->shutdown_) {
    rd_kafka_message_t *msg = rd_kafka_consume( rkt, 0, 1000 );
    if( msg ) {
      if( msg->err==0 ) {
        cl->Consume( std::string(cl->repo_name_), std::string((char*)msg->payload) );
      } 
      
      rd_kafka_message_destroy(msg); 
    }
  }
  LogCvmfs( kLogCvmfs, kLogDebug, "KafkaNotificationClient - Exited message loop" );

  rd_kafka_topic_destroy(rkt);
  rd_kafka_destroy(rk);


  return NULL;
}

#undef ERRMSG
