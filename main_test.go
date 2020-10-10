package main

import (
	"testing"
	"time"
)

func Test_csvFileName(t *testing.T) {
	type args struct {
		csvPrefix string
		splitAt   string
		now       time.Time
	}

	loc, err := time.LoadLocation("Local")
	if err != nil {
		t.Error(err)
		return
	}

	now8 := time.Date(2020, time.October, 10, 8, 0, 0, 0, loc)
	now10 := time.Date(2020, time.October, 10, 10, 0, 0, 0, loc)

	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{name: "at 8:00", args: args{csvPrefix: "adsb", splitAt: "9:00", now: now8}, want: "adsb-2020-10-09T0900.csv", wantErr: false},
		{name: "at 10:00", args: args{csvPrefix: "adsb", splitAt: "9:00", now: now10}, want: "adsb-2020-10-10T0900.csv", wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := csvFileName(tt.args.csvPrefix, tt.args.splitAt, tt.args.now)
			if (err != nil) != tt.wantErr {
				t.Errorf("csvFileName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("csvFileName() got = %v, want %v", got, tt.want)
			}
		})
	}
}
