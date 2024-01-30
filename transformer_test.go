package redis_autopipeline

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

func TestTransformDel(t *testing.T) {
	tests := []struct {
		name string
		keys []string
		want []string
	}{
		{
			name: "Del multiple keys",
			keys: []string{"elem1", "elem2"},
			want: []string{"elem1", "elem2"},
		},
		{
			name: "Del one key",
			keys: []string{"elem1"},
			want: []string{"elem1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := transformDel(tt.keys...)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTransformMGet(t *testing.T) {
	tests := []struct {
		name string
		keys []string
		want []string
	}{
		{
			name: "MGet multiple keys",
			keys: []string{"elem1", "elem2"},
			want: []string{"elem1", "elem2"},
		},
		{
			name: "MGet one key",
			keys: []string{"elem1"},
			want: []string{"elem1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := transformMGet(tt.keys...)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTransformHDel(t *testing.T) {
	tests := []struct {
		name      string
		operation operationPrefix
		key       string
		fields    []string
		want      []string
	}{
		{
			name:      "HDel with values",
			operation: HDel,
			key:       "somekey",
			fields:    []string{"elem1", "elem2"},
			want:      []string{"somekey", "elem1", "elem2"},
		},
		{
			name:      "HDel, no values",
			operation: HDel,
			key:       "somekey",
			want:      []string{"somekey"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := transformHDel(tt.key, tt.fields...)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTransformHGet(t *testing.T) {
	tests := []struct {
		name  string
		key   string
		field string
		want  []string
	}{
		{
			name:  "HGet ok",
			key:   "somekey",
			field: "somehash",
			want:  []string{"somekey", "somehash"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := transformHGet(tt.key, tt.field)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTransformGet(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want []string
	}{
		{
			name: "Get ok",
			key:  "somekey",
			want: []string{"somekey"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := transformGet(tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTransformHGetAll(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want []string
	}{
		{
			name: "HGetAll ok",
			key:  "somekey",
			want: []string{"somekey"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := transformHGetAll(tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTransformSMembers(t *testing.T) {
	tests := []struct {
		name string
		key  string
		want []string
	}{
		{
			name: "SMembers ok",
			key:  "somekey",
			want: []string{"somekey"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := transformSMembers(tt.key)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestTransformExpire(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		expiration time.Duration
		want       []string
		wantErr    string
	}{
		{
			name:       "Expire ok",
			key:        "somekey",
			expiration: time.Microsecond,
			want:       []string{"somekey", strconv.Itoa(int(time.Microsecond))},
			wantErr:    "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := transformExpire(tt.key, tt.expiration)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNormalizeHDel(t *testing.T) {
	tests := []struct {
		name       string
		values     []string
		wantKey    string
		wantFields []string
	}{
		{
			name:    "just key",
			values:  []string{"key"},
			wantKey: "key",
		},
		{
			name:       "key and fields",
			values:     []string{"key", "v1", "v2"},
			wantKey:    "key",
			wantFields: []string{"v1", "v2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, fs := normalizeHDel(tt.values)
			assert.Equal(t, tt.wantKey, key)
			assert.Equal(t, tt.wantFields, fs)
		})
	}
}

func TestNormalizeDel(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   []string
	}{
		{
			name:   "key and fields",
			values: []string{"v0", "v1", "v2"},
			want:   []string{"v0", "v1", "v2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeDel(tt.values)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNormalizeMGet(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   []string
	}{
		{
			name:   "key and fields",
			values: []string{"v0", "v1", "v2"},
			want:   []string{"v0", "v1", "v2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeMGet(tt.values)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNormalizeHGet(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		key    string
		field  string
	}{
		{
			name:   "key and fields",
			values: []string{"key", "f1"},
			key:    "key",
			field:  "f1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, field := normalizeHGet(tt.values)
			assert.Equal(t, tt.key, key)
			assert.Equal(t, tt.field, field)
		})
	}
}

func TestNormalizeGet(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   string
	}{
		{
			name:   "key and fields",
			values: []string{"key"},
			want:   "key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeGet(tt.values)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNormalizeHGetAll(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   string
	}{
		{
			name:   "key and fields",
			values: []string{"key"},
			want:   "key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeHGetAll(tt.values)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNormalizeSMembers(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   string
	}{
		{
			name:   "key and fields",
			values: []string{"key"},
			want:   "key",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := normalizeGet(tt.values)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestNormalizeExpire(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		key    string
		exp    time.Duration
	}{
		{
			name:   "nanosecond",
			values: []string{"key", "1"},
			key:    "key",
			exp:    time.Nanosecond,
		},
		{
			name:   "1000 nanosecond",
			values: []string{"key", "1000"},
			key:    "key",
			exp:    time.Nanosecond * 1000,
		},
		{
			name:   "microsecond",
			values: []string{"key", "1000"},
			key:    "key",
			exp:    time.Microsecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, exp := normalizeExpire(tt.values)
			assert.Equal(t, tt.key, key)
			assert.Equal(t, tt.exp, exp)
		})
	}
}
