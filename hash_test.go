package redis_autopipeline

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHashStringSlice(t *testing.T) {
	tests := []struct {
		name     string
		in       []string
		expected string
	}{
		{
			"OK: a,b,c",
			[]string{"a", "b", "c"},
			"6b23e5291e4989b8a7684eafdee18b55b0a2cab016d4a42a3feb7d70157c8a4f",
		},
		{
			"OK: abc",
			[]string{"abc"},
			"5b14979300b6bec9418599a4f0b5770cb99c4f20ae4914a178f0e5025a3d925e",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := hashStringSlice(HDel, tt.in)
			assert.Equal(t, tt.expected, got)
			got2 := hashStringSlice(Del, tt.in)
			assert.NotEqual(t, got, got2)
		})

	}
}
