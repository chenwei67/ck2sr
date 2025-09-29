package scheduler

import (
	"time"
)

type TimeWindow struct {
	StartTime time.Time
	EndTime   time.Time
}

func NewTimeWindow(start, end time.Time) *TimeWindow {
	return &TimeWindow{
		StartTime: start,
		EndTime:   end,
	}
}

func (w *TimeWindow) Contains(t time.Time) bool {
	return !t.Before(w.StartTime) && !t.After(w.EndTime)
}

func (w *TimeWindow) IsActive() bool {
	now := time.Now()
	return w.Contains(now)
}

func (w *TimeWindow) Duration() time.Duration {
	return w.EndTime.Sub(w.StartTime)
}

func ParseTimeWindow(startStr, endStr string) (*TimeWindow, error) {
	layout := "15:04:05"

	start, err := time.Parse(layout, startStr)
	if err != nil {
		return nil, err
	}

	end, err := time.Parse(layout, endStr)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	startTime := time.Date(now.Year(), now.Month(), now.Day(),
		start.Hour(), start.Minute(), start.Second(), 0, now.Location())
	endTime := time.Date(now.Year(), now.Month(), now.Day(),
		end.Hour(), end.Minute(), end.Second(), 0, now.Location())

	return NewTimeWindow(startTime, endTime), nil
}

type TimeWindowManager struct {
	windows []*TimeWindow
}

func NewTimeWindowManager() *TimeWindowManager {
	return &TimeWindowManager{
		windows: make([]*TimeWindow, 0),
	}
}

func (m *TimeWindowManager) AddWindow(window *TimeWindow) {
	m.windows = append(m.windows, window)
}

func (m *TimeWindowManager) IsInAnyWindow(t time.Time) bool {
	for _, window := range m.windows {
		if window.Contains(t) {
			return true
		}
	}
	return false
}

func (m *TimeWindowManager) IsCurrentlyActive() bool {
	return m.IsInAnyWindow(time.Now())
}