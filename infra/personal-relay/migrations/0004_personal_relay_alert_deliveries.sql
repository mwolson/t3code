CREATE TABLE personal_relay_alert_deliveries (
  alert_id text NOT NULL REFERENCES personal_relay_alerts (alert_id) ON DELETE CASCADE,
  device_id text NOT NULL,
  status text NOT NULL CHECK (status IN ('pending', 'accepted', 'suppressed',
    'superseded', 'expired', 'rejected', 'ambiguous_abandoned')),
  unknown_sends integer NOT NULL DEFAULT 0,
  last_channel text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (alert_id, device_id)
);
--> statement-breakpoint
CREATE INDEX personal_relay_alert_deliveries_pending
  ON personal_relay_alert_deliveries (device_id) WHERE status = 'pending';
