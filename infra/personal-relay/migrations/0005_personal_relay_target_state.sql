CREATE TABLE personal_relay_target_state (
  device_id text NOT NULL,
  channel text NOT NULL CHECK (channel IN ('live_activity', 'notification')),
  failure_count integer NOT NULL DEFAULT 0,
  retry_at timestamptz,
  last_error text,                        -- sanitized class and APNs reason only
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (device_id, channel)
);
