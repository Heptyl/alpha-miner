CREATE TABLE IF NOT EXISTS dataset_snapshots (
 source_snapshot_sha256 TEXT PRIMARY KEY, manifest_json TEXT NOT NULL CHECK(json_valid(manifest_json)),
 size_bytes INTEGER NOT NULL CHECK(size_bytes>0), latest_trade_date TEXT NOT NULL,
 published_at TEXT NOT NULL, bound_at TEXT NOT NULL, schema_version INTEGER NOT NULL CHECK(schema_version=1),
 CHECK(length(source_snapshot_sha256)=64 AND source_snapshot_sha256 NOT GLOB '*[^0-9a-f]*')
);
CREATE TABLE IF NOT EXISTS research_candidates (
 candidate_hash TEXT PRIMARY KEY, candidate_name TEXT NOT NULL, experiment_type TEXT NOT NULL,
 code_text TEXT NOT NULL, code_hash TEXT NOT NULL, parameters_json TEXT NOT NULL CHECK(json_valid(parameters_json)),
 parameters_hash TEXT NOT NULL, data_manifest_json TEXT NOT NULL CHECK(json_valid(data_manifest_json)),
 data_hash TEXT NOT NULL, dataset_snapshot_hash TEXT NOT NULL, cost_model_json TEXT NOT NULL CHECK(json_valid(cost_model_json)),
 cost_hash TEXT NOT NULL, protocol_json TEXT NOT NULL CHECK(json_valid(protocol_json)), protocol_hash TEXT NOT NULL,
 parent_hashes_json TEXT NOT NULL CHECK(json_valid(parent_hashes_json)), lineage_roots_json TEXT NOT NULL CHECK(json_valid(lineage_roots_json)),
 lineage_hash TEXT NOT NULL, frozen_at TEXT NOT NULL, schema_version INTEGER NOT NULL CHECK(schema_version=1),
 CHECK(length(candidate_hash)=64 AND candidate_hash NOT GLOB '*[^0-9a-f]*'),
 CHECK(length(code_hash)=64 AND code_hash NOT GLOB '*[^0-9a-f]*'),
 CHECK(length(parameters_hash)=64 AND parameters_hash NOT GLOB '*[^0-9a-f]*'),
 CHECK(length(data_hash)=64 AND data_hash NOT GLOB '*[^0-9a-f]*'),
 CHECK(length(cost_hash)=64 AND cost_hash NOT GLOB '*[^0-9a-f]*'),
 CHECK(length(protocol_hash)=64 AND protocol_hash NOT GLOB '*[^0-9a-f]*'),
 CHECK(length(lineage_hash)=64 AND lineage_hash NOT GLOB '*[^0-9a-f]*'),
 FOREIGN KEY(dataset_snapshot_hash) REFERENCES dataset_snapshots(source_snapshot_sha256)
);
CREATE TABLE IF NOT EXISTS research_evidence (
 sequence_id INTEGER PRIMARY KEY AUTOINCREMENT, event_id TEXT NOT NULL UNIQUE, idempotency_key TEXT NOT NULL UNIQUE,
 candidate_hash TEXT NOT NULL, lineage_hash TEXT NOT NULL,
 event_type TEXT NOT NULL CHECK(event_type IN ('DEVELOPMENT_RESULT','HOLDOUT_OPENED','HOLDOUT_RESULT','EVALUATION_ERROR')),
 holdout_scope_hash TEXT,
 payload_json TEXT NOT NULL CHECK(json_valid(payload_json)), payload_hash TEXT NOT NULL, recorded_at TEXT NOT NULL,
 CHECK(length(event_id)=64 AND event_id NOT GLOB '*[^0-9a-f]*'),
 CHECK(length(lineage_hash)=64 AND lineage_hash NOT GLOB '*[^0-9a-f]*'),
 CHECK(length(payload_hash)=64 AND payload_hash NOT GLOB '*[^0-9a-f]*'),
 UNIQUE(candidate_hash,event_type), FOREIGN KEY(candidate_hash) REFERENCES research_candidates(candidate_hash)
);
CREATE INDEX IF NOT EXISTS idx_research_candidates_lineage ON research_candidates(lineage_hash);
CREATE INDEX IF NOT EXISTS idx_research_evidence_candidate ON research_evidence(candidate_hash,sequence_id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_research_holdout_once_per_lineage ON research_evidence(lineage_hash) WHERE event_type='HOLDOUT_OPENED';
CREATE UNIQUE INDEX IF NOT EXISTS idx_research_holdout_once_per_scope ON research_evidence(holdout_scope_hash) WHERE event_type='HOLDOUT_OPENED';
CREATE TRIGGER IF NOT EXISTS trg_dataset_snapshots_no_update BEFORE UPDATE ON dataset_snapshots BEGIN SELECT RAISE(ABORT,'dataset_snapshots is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_dataset_snapshots_no_delete BEFORE DELETE ON dataset_snapshots BEGIN SELECT RAISE(ABORT,'dataset_snapshots is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_research_candidates_no_update BEFORE UPDATE ON research_candidates BEGIN SELECT RAISE(ABORT,'research_candidates is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_research_candidates_no_delete BEFORE DELETE ON research_candidates BEGIN SELECT RAISE(ABORT,'research_candidates is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_research_evidence_no_update BEFORE UPDATE ON research_evidence BEGIN SELECT RAISE(ABORT,'research_evidence is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_research_evidence_no_delete BEFORE DELETE ON research_evidence BEGIN SELECT RAISE(ABORT,'research_evidence is append-only'); END;
CREATE TRIGGER IF NOT EXISTS trg_candidate_snapshot_exists BEFORE INSERT ON research_candidates WHEN NOT EXISTS(SELECT 1 FROM dataset_snapshots WHERE source_snapshot_sha256=NEW.dataset_snapshot_hash) BEGIN SELECT RAISE(ABORT,'research candidate snapshot is not registered'); END;
CREATE TRIGGER IF NOT EXISTS trg_evidence_candidate_exists BEFORE INSERT ON research_evidence WHEN NOT EXISTS(SELECT 1 FROM research_candidates WHERE candidate_hash=NEW.candidate_hash) BEGIN SELECT RAISE(ABORT,'research evidence candidate does not exist'); END;
CREATE TRIGGER IF NOT EXISTS trg_evidence_lineage_matches BEFORE INSERT ON research_evidence WHEN NEW.lineage_hash!=(SELECT lineage_hash FROM research_candidates WHERE candidate_hash=NEW.candidate_hash) BEGIN SELECT RAISE(ABORT,'research evidence lineage mismatch'); END;
CREATE TRIGGER IF NOT EXISTS trg_holdout_requires_development BEFORE INSERT ON research_evidence WHEN NEW.event_type='HOLDOUT_OPENED' AND NOT EXISTS(SELECT 1 FROM research_evidence WHERE candidate_hash=NEW.candidate_hash AND event_type='DEVELOPMENT_RESULT') BEGIN SELECT RAISE(ABORT,'holdout requires development result'); END;
CREATE TRIGGER IF NOT EXISTS trg_result_requires_open BEFORE INSERT ON research_evidence WHEN NEW.event_type IN ('HOLDOUT_RESULT','EVALUATION_ERROR') AND NOT EXISTS(SELECT 1 FROM research_evidence WHERE candidate_hash=NEW.candidate_hash AND event_type='HOLDOUT_OPENED') BEGIN SELECT RAISE(ABORT,'holdout terminal requires opened event'); END;
CREATE TRIGGER IF NOT EXISTS trg_holdout_scope_matches BEFORE INSERT ON research_evidence WHEN NEW.event_type='HOLDOUT_OPENED' AND (NEW.holdout_scope_hash IS NULL OR length(NEW.holdout_scope_hash)!=64 OR NEW.holdout_scope_hash!=json_extract((SELECT protocol_json FROM research_candidates WHERE candidate_hash=NEW.candidate_hash),'$.holdout_scope_hash')) BEGIN SELECT RAISE(ABORT,'holdout scope does not match frozen protocol'); END;
CREATE TRIGGER IF NOT EXISTS trg_terminal_once BEFORE INSERT ON research_evidence WHEN NEW.event_type IN ('HOLDOUT_RESULT','EVALUATION_ERROR') AND EXISTS(SELECT 1 FROM research_evidence WHERE candidate_hash=NEW.candidate_hash AND event_type IN ('HOLDOUT_RESULT','EVALUATION_ERROR')) BEGIN SELECT RAISE(ABORT,'holdout terminal already exists'); END;
CREATE TRIGGER IF NOT EXISTS trg_development_rejects_retired BEFORE INSERT ON research_evidence WHEN NEW.event_type='DEVELOPMENT_RESULT' AND EXISTS(SELECT 1 FROM research_candidates incoming CROSS JOIN json_each(incoming.lineage_roots_json) ir JOIN research_evidence opened ON opened.event_type='HOLDOUT_OPENED' JOIN research_candidates oc ON oc.candidate_hash=opened.candidate_hash CROSS JOIN json_each(oc.lineage_roots_json) oroot WHERE incoming.candidate_hash=NEW.candidate_hash AND ir.value=oroot.value) BEGIN SELECT RAISE(ABORT,'retired lineage rejects development result'); END;
CREATE TRIGGER IF NOT EXISTS trg_holdout_rejects_overlap BEFORE INSERT ON research_evidence WHEN NEW.event_type='HOLDOUT_OPENED' AND EXISTS(SELECT 1 FROM research_candidates incoming CROSS JOIN json_each(incoming.lineage_roots_json) ir JOIN research_evidence opened ON opened.event_type='HOLDOUT_OPENED' JOIN research_candidates oc ON oc.candidate_hash=opened.candidate_hash CROSS JOIN json_each(oc.lineage_roots_json) oroot WHERE incoming.candidate_hash=NEW.candidate_hash AND ir.value=oroot.value) BEGIN SELECT RAISE(ABORT,'holdout lineage root has already been opened'); END;
