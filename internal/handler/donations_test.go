package handler

import "testing"

func TestCalculateDisplayDuration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		amount  int64
		rate    int64
		min     int
		max     int
		want    int
		wantErr bool
	}{
		{name: "streamer rate", amount: 10_000, rate: 1_000, min: 5, max: 60, want: 10},
		{name: "integer IDR floors partial second", amount: 10_999, rate: 1_000, min: 5, max: 60, want: 10},
		{name: "minimum clamp", amount: 1_000, rate: 1_000, min: 5, max: 60, want: 5},
		{name: "maximum clamp", amount: 100_000, rate: 1_000, min: 5, max: 60, want: 60},
		{name: "invalid rate", amount: 10_000, rate: 0, min: 5, max: 60, wantErr: true},
		{name: "invalid bounds", amount: 10_000, rate: 1_000, min: 0, max: 60, wantErr: true},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := calculateDisplayDuration(tt.amount, tt.rate, tt.min, tt.max)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Fatalf("duration = %d, want %d", got, tt.want)
			}
		})
	}
}
