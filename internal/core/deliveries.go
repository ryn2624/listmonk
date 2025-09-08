package core

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/lib/pq"
)

// RecordDelivery records a new delivery event. If subscriber or campaign cannot be resolved, it is dropped silently (log-only).
// Idempotent on ses_message_id.
func (c *Core) RecordDelivery(subUUID, email, campUUID, sesMsgID, source string, meta json.RawMessage, createdAt time.Time) error {
	if sesMsgID == "" {
		return echo.NewHTTPError(http.StatusBadRequest, c.i18n.T("globals.messages.invalidData"))
	}

	if createdAt.IsZero() {
		createdAt = time.Now()
	}

	_, err := c.q.RecordDelivery.Exec(subUUID, email, campUUID, sesMsgID, source, meta, createdAt)
	if err != nil {
		// Drop unresolvable events: if either subscriber or campaign cannot be resolved, the insert will fail with null violation.
		if pqErr, ok := err.(*pq.Error); ok {
			c.log.Printf("delivery event dropped (sub/camp not found) for email=%s camp=%s: %v", email, campUUID, pqErr)
			return nil
		}
		c.log.Printf("error recording delivery: %v", err)
		return echo.NewHTTPError(http.StatusInternalServerError, c.i18n.Ts("globals.messages.errorSaving", "name", "delivery"))
	}
	return nil
}
