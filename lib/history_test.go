package lib

import "testing"

func TestSum(t *testing.T) {
	he := HistoricalExecution{}

	err := RenderHistoryResults([]HistoricalExecution{he}, "csv")
	if err != nil {
		t.Errorf("Unable to render as %s", "csv")
	}
}
