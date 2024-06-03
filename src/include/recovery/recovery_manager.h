#ifndef MINISQL_RECOVERY_MANAGER_H
#define MINISQL_RECOVERY_MANAGER_H

#include <map>
#include <unordered_map>
#include <vector>

#include "recovery/log_rec.h"

using KvDatabase = std::unordered_map<KeyType, ValType>;
using ATT = std::unordered_map<txn_id_t, lsn_t>;

struct CheckPoint {
    lsn_t checkpoint_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase persist_data_{};

    inline void AddActiveTxn(txn_id_t txn_id, lsn_t last_lsn) { active_txns_[txn_id] = last_lsn; }

    inline void AddData(KeyType key, ValType val) { persist_data_.emplace(std::move(key), val); }
};

class RecoveryManager {
public:
    /**
    * TODO: Student Implement
    */
    void Init(CheckPoint &last_checkpoint) {
        persist_lsn_=last_checkpoint.checkpoint_lsn_;
        active_txns_=std::move(last_checkpoint.active_txns_);
        data_=std::move(last_checkpoint.persist_data_);
    }

    /**
    * TODO: Student Implement
    */
    void RedoPhase() {
        for(auto it:log_recs_){
            if(it.first<persist_lsn_)continue;
            LogRecPtr log=it.second;
            active_txns_[log->txn_id]=it.first;
            if(log->type_==LogRecType::kInsert){
                data_.emplace(log->inskey,log->insval);
            }else if(log->type_==LogRecType::kDelete){
                data_.erase(log->delkey);
            }else if(log->type_==LogRecType::kUpdate){
                data_.erase(log->uptoldkey);
                data_.emplace(log->uptnewkey,log->uptnewval);
            }else if(log->type_==LogRecType::kCommit){
                active_txns_.erase(log->txn_id);
            }else if(log->type_==LogRecType::kAbort){
                Rollback(log->txn_id);
                active_txns_.erase(log->txn_id);
            }
        }
    }

    /**
    * TODO: Student Implement
    */
    void UndoPhase() {
        for(auto it:active_txns_){
            Rollback(it.first);
        }
        active_txns_.clear();
    }
    void Rollback(txn_id_t txn_id){
        auto log_id=active_txns_[txn_id];
        while(log_id!=INVALID_LSN){
            LogRecPtr log=log_recs_[log_id];
            if(log==nullptr)break;
            if(log->type_==LogRecType::kInsert){
                data_.erase(log->inskey);
            }else if(log->type_==LogRecType::kDelete){
                data_.emplace(log->delkey,log->delval);
            }else if(log->type_==LogRecType::kUpdate){
                data_.erase(log->uptnewkey);
                data_.emplace(log->uptoldkey,log->uptoldval);
            }
            log_id=log->prev_lsn_;
        }
    }
    // used for test only
    void AppendLogRec(LogRecPtr log_rec) { log_recs_.emplace(log_rec->lsn_, log_rec); }

    // used for test only
    inline KvDatabase &GetDatabase() { return data_; }

private:
    std::map<lsn_t, LogRecPtr> log_recs_{};
    lsn_t persist_lsn_{INVALID_LSN};
    ATT active_txns_{};
    KvDatabase data_{};  // all data in database
};

#endif  // MINISQL_RECOVERY_MANAGER_H
