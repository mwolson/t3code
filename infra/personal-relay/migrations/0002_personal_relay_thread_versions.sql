CREATE TABLE personal_relay_thread_versions (
  environment_id text NOT NULL,
  environment_public_key text NOT NULL,
  thread_id text NOT NULL,
  last_seq bigint NOT NULL,
  removed boolean NOT NULL,
  transition_id text,                     -- from t3_transition, for supersession
  accepted_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (environment_id, environment_public_key, thread_id)
);
