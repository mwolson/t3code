CREATE TABLE personal_relay_accounts (
  user_id text PRIMARY KEY,
  generation bigint NOT NULL DEFAULT 0,   -- fence only (1.1)
  next_reconcile_at timestamptz,          -- diagnostics; the worker recomputes it
  last_pass_at timestamptz,
  last_pass_error text,                   -- sanitized
  updated_at timestamptz NOT NULL DEFAULT now()
);
