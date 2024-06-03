#ifndef MINISQL_LOG_REC_H
#define MINISQL_LOG_REC_H

#include <unordered_map>
#include <utility>

#include "common/config.h"
#include "common/rowid.h"
#include "record/row.h"

enum class LogRecType {
    kInvalid,
    kInsert,
    kDelete,
    kUpdate,
    kBegin,
    kCommit,
    kAbort,
};

// used for testing only
using KeyType = std::string;
using ValType = int32_t;

/**
 * TODO: Student Implement
 */
struct LogRec {
    LogRec() = default;

    LogRecType type_{LogRecType::kInvalid};
    lsn_t lsn_{INVALID_LSN};
    lsn_t prev_lsn_{INVALID_LSN};
    txn_id_t txn_id={INVALID_TXN_ID};

    KeyType inskey;
    ValType  insval;
    KeyType delkey;
    ValType  delval;
    KeyType uptoldkey;
    ValType  uptoldval;
    KeyType uptnewkey;
    ValType  uptnewval; 

    /* used for testing only */
    static std::unordered_map<txn_id_t, lsn_t> prev_lsn_map_;
    static lsn_t next_lsn_;

    static lsn_t adjustlsn(txn_id_t id,lsn_t lsn){
        auto it=prev_lsn_map_.find(id);
        lsn_t las_lsn=INVALID_LSN;
        if(it!=prev_lsn_map_.end()){
            las_lsn=it->second;
            it->second=lsn;
        }
        else prev_lsn_map_.emplace(id,lsn);
        return las_lsn;
    }
};

std::unordered_map<txn_id_t, lsn_t> LogRec::prev_lsn_map_ = {};
lsn_t LogRec::next_lsn_ = 0;

typedef std::shared_ptr<LogRec> LogRecPtr;

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateInsertLog(txn_id_t txn_id, KeyType ins_key, ValType ins_val) {
    lsn_t lsn=LogRec::next_lsn_++;
    LogRecPtr res=std::make_shared<LogRec>();
    res->txn_id=txn_id;
    res->type_=LogRecType::kInsert;
    res->lsn_=lsn;
    res->prev_lsn_=LogRec::adjustlsn(txn_id,lsn);
    res->inskey=ins_key;
    res->insval=ins_val;
    return res;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateDeleteLog(txn_id_t txn_id, KeyType del_key, ValType del_val) {
    lsn_t lsn=LogRec::next_lsn_++;
    LogRecPtr res=std::make_shared<LogRec>();
    res->txn_id=txn_id;
    res->type_=LogRecType::kDelete;
    res->lsn_=lsn;
    res->prev_lsn_=LogRec::adjustlsn(txn_id,lsn);
    res->delkey=del_key;
    res->delval=del_val;
    return res;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateUpdateLog(txn_id_t txn_id, KeyType old_key, ValType old_val, KeyType new_key, ValType new_val) {
    lsn_t lsn=LogRec::next_lsn_++;
    LogRecPtr res=std::make_shared<LogRec>();
    res->txn_id=txn_id;
    res->type_=LogRecType::kUpdate;
    res->lsn_=lsn;
    res->prev_lsn_=LogRec::adjustlsn(txn_id,lsn);
    res->uptoldkey=old_key;
    res->uptoldval=old_val;
    res->uptnewkey=new_key;
    res->uptnewval=new_val;
    return res;
}
/**
 * TODO: Student Implement
 */
static LogRecPtr CreateBeginLog(txn_id_t txn_id) {
    lsn_t lsn=LogRec::next_lsn_++;
    LogRecPtr res=std::make_shared<LogRec>();
    res->txn_id=txn_id;
    res->type_=LogRecType::kBegin;
    res->lsn_=lsn;
    res->prev_lsn_=LogRec::adjustlsn(txn_id,lsn);
    return res;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateCommitLog(txn_id_t txn_id) {
    lsn_t lsn=LogRec::next_lsn_++;
    LogRecPtr res=std::make_shared<LogRec>();
    res->txn_id=txn_id;
    res->type_=LogRecType::kCommit;
    res->lsn_=lsn;
    res->prev_lsn_=LogRec::adjustlsn(txn_id,lsn);
    return res;
}

/**
 * TODO: Student Implement
 */
static LogRecPtr CreateAbortLog(txn_id_t txn_id) {
    lsn_t lsn=LogRec::next_lsn_++;
    LogRecPtr res=std::make_shared<LogRec>();
    res->txn_id=txn_id;
    res->type_=LogRecType::kAbort;
    res->lsn_=lsn;
    res->prev_lsn_=LogRec::adjustlsn(txn_id,lsn);
    return res;
}

#endif  // MINISQL_LOG_REC_H
