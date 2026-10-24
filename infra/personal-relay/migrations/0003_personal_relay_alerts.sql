CREATE TABLE personal_relay_alerts (
  alert_id text PRIMARY KEY,              -- sha256(environment_id, thread_id, transition_id)
  user_id text NOT NULL,
  environment_id text NOT NULL,
  environment_public_key text NOT NULL,
  thread_id text NOT NULL,
  transition_id text NOT NULL,
  phase text NOT NULL,
  transition_at timestamptz NOT NULL,
  eligible_at timestamptz NOT NULL,        -- creation window and lifetime (addendum C-2)
  row_json jsonb NOT NULL,                -- aggregate row snapshot at acceptance
  created_at timestamptz NOT NULL DEFAULT now(),
  expires_at timestamptz NOT NULL         -- eligible_at + 30 minutes
);
--> statement-breakpoint
CREATE INDEX personal_relay_alerts_environment ON personal_relay_alerts (environment_id, environment_public_key);
