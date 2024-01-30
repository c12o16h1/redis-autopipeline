package redis_autopipeline

import (
	"strconv"
	"time"
)

// transformHDel transforms HDel arguments to slice of strings
func transformHDel(key string, fields ...string) []string {
	if len(fields) == 0 {
		return []string{key}
	}
	stringSlice := make([]string, len(fields)+1)
	stringSlice[0] = key
	for i, v := range fields {
		stringSlice[i+1] = v
	}
	return stringSlice
}

// normalizeHDel transforms string slice to a valid HDel redis arguments
func normalizeHDel(values []string) (string, []string) {
	// payload is a key string as first param and strings slice as an optional second param
	if len(values) == 1 {
		return values[0], nil
	}
	return values[0], values[1:]
}

// transformDel transforms Del arguments to slice of strings
func transformDel(keys ...string) []string {
	// payload is strings slice
	return keys
}

// normalizeDel transforms string slice to a valid Del redis arguments
func normalizeDel(values []string) []string {
	// payload is strings slice
	return values
}

// transformMGet transforms MGet arguments to slice of strings
func transformMGet(keys ...string) []string {
	// payload is strings slice
	return keys
}

// normalizeMGet transforms string slice to a valid MGet redis arguments
func normalizeMGet(values []string) []string {
	// payload is strings slice
	return values
}

// transformHGet transforms HGet arguments to slice of strings
func transformHGet(key, field string) []string {
	// payload is two strings
	return []string{key, field}
}

// normalizeHGet transforms string slice to a valid HGet redis arguments
func normalizeHGet(values []string) (string, string) {
	// payload is two strings
	return values[0], values[1]
}

// transformGet transforms Get arguments to slice of strings
func transformGet(key string) []string {
	// payload is one string
	return []string{key}
}

// normalizeGet transforms string slice to a valid Get redis arguments
func normalizeGet(values []string) string {
	// payload is one string
	return values[0]
}

// transformHGetAll transforms HGetAll arguments to slice of strings
func transformHGetAll(key string) []string {
	// payload is one string
	return []string{key}
}

// normalizeHGetAll transforms string slice to a valid HGetAll redis arguments
func normalizeHGetAll(values []string) string {
	// payload is one string
	return values[0]
}

// transformSMembers transforms SMembers arguments to slice of strings
func transformSMembers(key string) []string {
	// payload is one string
	return []string{key}
}

// normalizeSMembers transforms string slice to a valid SMembers redis arguments
func normalizeSMembers(values []string) string {
	// payload is one string
	return values[0]
}

// transformExpire transforms Expire arguments to slice of strings
func transformExpire(key string, expiration time.Duration) []string {
	// payload is a key string as first param and time.Duration as second param
	return []string{key, strconv.Itoa(int(expiration.Nanoseconds()))}
}

// normalizeExpire transforms string slice to a valid Expire redis arguments
func normalizeExpire(values []string) (string, time.Duration) {
	// payload is string and time.Duration
	nanoseconds, _ := strconv.Atoi(values[1])
	return values[0], time.Duration(nanoseconds)
}
