package eth_oracle

import (
	"testing"
)

func Test_bestHeight(t *testing.T) {
	type args struct {
		configured int64
		hardcoded  int64
		lastStored int64
	}
	tests := []struct {
		name string
		args args
		want int64
	}{
		{
			name: "configured is the best",
			args: args{
				configured: 10,
				hardcoded:  5,
				lastStored: 5,
			},
			want: 10,
		},
		{
			name: "hardcoded is the best",
			args: args{
				configured: 5,
				hardcoded:  10,
				lastStored: 5,
			},
			want: 10,
		},
		{
			name: "lastStored is the best",
			args: args{
				configured: 5,
				hardcoded:  5,
				lastStored: 10,
			},
			want: 10,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := bestHeight(tt.args.configured, tt.args.hardcoded, tt.args.lastStored); got != tt.want {
				t.Errorf("bestHeight() = %v, want %v", got, tt.want)
			}
		})
	}
}
