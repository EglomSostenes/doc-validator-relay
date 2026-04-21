# Migration: Add `processing` status to outbox_events
#
# Run this inside the Rails project. No schema change is needed in Postgres
# because the status column is already an integer — we only update the enum
# definition in the Rails model.
#
# 1. Add the value to the Rails model enum
# ----------------------------------------------------------------------------
# In app/models/outbox_event.rb, change:
#
#   enum :status, { pending: 0, published: 1, failed: 2 }, prefix: :status
#
# to:
#
#   enum :status, { pending: 0, published: 1, failed: 2, processing: 3 }, prefix: :status
#
#
# 2. No database migration required
# ----------------------------------------------------------------------------
# The column is an integer. Postgres stores 3 for :processing transparently.
# The Rails migration below is provided only as documentation / for teams that
# prefer explicit migrations even for enum additions.
#
# class AddProcessingStatusToOutboxEvents < ActiveRecord::Migration[7.1]
#   def up
#     # No-op: integer enum values need no schema change.
#     # This migration exists to document the contract between Rails and the
#     # Go relay service: status=3 means "claimed by the relay, in flight".
#   end
#
#   def down
#     # Reset any stuck processing events to pending before removing the value.
#     execute "UPDATE outbox_events SET status = 0 WHERE status = 3"
#   end
# end
#
#
# 3. Feature flag (optional, for gradual rollout)
# ----------------------------------------------------------------------------
# While both Rails and Go relay are running simultaneously, guard the Rails
# OutboxRelayJob with a Setting flag so only one side publishes at a time:
#
#   class OutboxRelayJob < ApplicationJob
#     def perform
#       return unless Setting.outbox_relay_rails_enabled?
#       # ... existing code
#     end
#   end
#
# Set Setting.outbox_relay_rails_enabled = false once the Go relay is stable.
