package lib

import (
	"testing"
	"time"
)

func TestParseTags(t *testing.T) {
	imageString := "https://registry.hub.docker.com/library/redis:*"
	image, err := ParseImage(imageString)
	if err != nil {
		t.Errorf("Error in parsing %s", imageString)
	}
	if image.Scheme != "https" {
		t.Errorf("Image string parsing error: wrong scheme: %v", image.Scheme)
	}
	if image.Registry != "registry.hub.docker.com" {
		t.Errorf("Image string parsing error: wrong registry: %v", image.Registry)
	}
	if image.Repository != "library/redis" {
		t.Errorf("Image string parsing error: wrong repository: %v", image.Repository)
	}
	if image.Tag != "*" {
		t.Errorf("Image string parsing error: wrong tag: %v", image.Tag)
	}
	if !image.TagWildcard {
		t.Errorf("Image string parsing error: no wildcard was parsed")
	}
}

func TestParseTagsWithGlob(t *testing.T) {
	imageString := "https://registry.hub.docker.com/vernemq/vernemq:1.9.2*"
	// 1.9.2-1
	// 1.9.2-1-alpine
	// 1.9.2
	// 1.9.2-alpine

	image, err := ParseImage(imageString)
	if err != nil {
		t.Errorf("Error in parsing %s", imageString)
	}

	if image.Scheme != "https" {
		t.Errorf("Image string parsing error: wrong scheme: %v", image.Scheme)
	}
	if image.Registry != "registry.hub.docker.com" {
		t.Errorf("Image string parsing error: wrong registry: %v", image.Registry)
	}
	if image.Repository != "vernemq/vernemq" {
		t.Errorf("Image string parsing error: %v", image.Repository)
	}
	if image.Tag != "1.9.2*" {
		t.Errorf("Image string parsing error: wrong tag: %v", image.Tag)
	}
}

func TestFilterUsingGlobStarMatchEverything(t *testing.T) {
	input := []string{"vnje", "nc.cnrje", "5230.25.83"}
	result, err := filterUsingGlob("*", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	if len(result) != len(input) {
		t.Errorf("The match is missing something, different lengths %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != input[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}

func TestFilterUsingGlobStar(t *testing.T) {
	input := []string{"aaaa", "aab.12-8", "2.3"}
	result, err := filterUsingGlob("a*", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"aaaa", "aab.12-8"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lengths %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}

func TestFilterUsingGlobAtBeginning(t *testing.T) {
	input := []string{"bar-foo", "ubuntu-foo", "foo-bar"}
	result, err := filterUsingGlob("*-foo", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"bar-foo", "ubuntu-foo"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lengths %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}

func TestFilterUsingGlobTwice(t *testing.T) {
	input := []string{"foo", "ubuntu-foo", "foo-bar", "version-foo-2", "nope"}
	result, err := filterUsingGlob("*foo*", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"foo", "ubuntu-foo", "foo-bar", "version-foo-2"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lengths %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}

func TestFilterUsingGlobRealLifeImages01(t *testing.T) {
	input := []string{"rhel6-m201911", "rhel6-m202001", "rhel6-m202002", "rhel6", "rhel7-m201911", "rhel7-m202001", "rhel7-m202002", "rhel7", "tmp-rhel6-m202002-20200213", "tmp-rhel7-m202002-20200213"}
	result, err := filterUsingGlob("rhel7-m*", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"rhel7-m201911", "rhel7-m202001", "rhel7-m202002"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lengths %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}

func TestFilterUsingGlobRealLifeImages02(t *testing.T) {
	input := []string{"rhel6-m201911", "rhel6-m202001", "rhel6-m202002", "rhel6", "rhel7-m201911", "rhel7-m202001", "rhel7-m202002", "rhel7", "tmp-rhel6-m202002-20200213", "tmp-rhel7-m202002-20200213"}
	result, err := filterUsingGlob("rhel7", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"rhel7"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lengths %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}

// --- Token cache tests ---

func TestExtractTokenCacheKey_ManifestURL(t *testing.T) {
	url := "https://registry.hub.docker.com/v2/library/redis/manifests/latest"
	key := extractTokenCacheKey(url, "")
	expected := "https://registry.hub.docker.com/v2/library/redis|"
	if key != expected {
		t.Errorf("Expected %q, got %q", expected, key)
	}
}

func TestExtractTokenCacheKey_BlobURL(t *testing.T) {
	url := "https://registry.hub.docker.com/v2/library/redis/blobs/sha256:abc123"
	key := extractTokenCacheKey(url, "")
	expected := "https://registry.hub.docker.com/v2/library/redis|"
	if key != expected {
		t.Errorf("Expected %q, got %q", expected, key)
	}
}

func TestExtractTokenCacheKey_TagsURL(t *testing.T) {
	url := "https://registry.hub.docker.com/v2/library/redis/tags/list"
	key := extractTokenCacheKey(url, "")
	expected := "https://registry.hub.docker.com/v2/library/redis|"
	if key != expected {
		t.Errorf("Expected %q, got %q", expected, key)
	}
}

func TestExtractTokenCacheKey_SameRepoSameKey(t *testing.T) {
	urls := []string{
		"https://registry.hub.docker.com/v2/library/redis/manifests/latest",
		"https://registry.hub.docker.com/v2/library/redis/blobs/sha256:abc123",
		"https://registry.hub.docker.com/v2/library/redis/tags/list",
	}
	keys := make(map[string]bool)
	for _, url := range urls {
		keys[extractTokenCacheKey(url, "myuser")] = true
	}
	if len(keys) != 1 {
		t.Errorf("Expected all URLs to produce the same cache key, got %d distinct keys: %v", len(keys), keys)
	}
}

func TestExtractTokenCacheKey_DifferentRepoDifferentKey(t *testing.T) {
	url1 := "https://registry.hub.docker.com/v2/library/redis/manifests/latest"
	url2 := "https://registry.hub.docker.com/v2/library/nginx/manifests/latest"
	key1 := extractTokenCacheKey(url1, "")
	key2 := extractTokenCacheKey(url2, "")
	if key1 == key2 {
		t.Errorf("Expected different cache keys for different repos, both got %q", key1)
	}
}

func TestExtractTokenCacheKey_DifferentUserDifferentKey(t *testing.T) {
	url := "https://registry.hub.docker.com/v2/library/redis/manifests/latest"
	key1 := extractTokenCacheKey(url, "alice")
	key2 := extractTokenCacheKey(url, "bob")
	if key1 == key2 {
		t.Errorf("Expected different cache keys for different users, both got %q", key1)
	}
}

func TestExtractTokenCacheKey_NoSuffix(t *testing.T) {
	url := "https://registry.hub.docker.com/v2/library/redis"
	key := extractTokenCacheKey(url, "user")
	expected := url + "|user"
	if key != expected {
		t.Errorf("Expected %q, got %q", expected, key)
	}
}

func clearTokenCache() {
	tokenCacheMu.Lock()
	defer tokenCacheMu.Unlock()
	tokenCacheMap = make(map[string]*cachedToken)
}

func TestGetSetCachedToken(t *testing.T) {
	clearTokenCache()
	defer clearTokenCache()

	key := "test-key"

	// Cache miss
	_, ok := getCachedToken(key)
	if ok {
		t.Error("Expected cache miss for empty cache")
	}

	// Set and retrieve
	setCachedToken(key, "Bearer abc123", 300)
	token, ok := getCachedToken(key)
	if !ok {
		t.Error("Expected cache hit after set")
	}
	if token != "Bearer abc123" {
		t.Errorf("Expected 'Bearer abc123', got %q", token)
	}
}

func TestCachedTokenExpiry(t *testing.T) {
	clearTokenCache()
	defer clearTokenCache()

	key := "expiry-test"

	// Set with a very short TTL by directly manipulating the cache
	tokenCacheMu.Lock()
	tokenCacheMap[key] = &cachedToken{
		token:     "Bearer expired",
		expiresAt: time.Now().Add(-1 * time.Second), // already expired
	}
	tokenCacheMu.Unlock()

	_, ok := getCachedToken(key)
	if ok {
		t.Error("Expected cache miss for expired token")
	}
}

func TestCachedTokenNotYetExpired(t *testing.T) {
	clearTokenCache()
	defer clearTokenCache()

	key := "fresh-test"

	tokenCacheMu.Lock()
	tokenCacheMap[key] = &cachedToken{
		token:     "Bearer fresh",
		expiresAt: time.Now().Add(5 * time.Minute),
	}
	tokenCacheMu.Unlock()

	token, ok := getCachedToken(key)
	if !ok {
		t.Error("Expected cache hit for non-expired token")
	}
	if token != "Bearer fresh" {
		t.Errorf("Expected 'Bearer fresh', got %q", token)
	}
}

func TestSetCachedTokenDefaultTTL(t *testing.T) {
	clearTokenCache()
	defer clearTokenCache()

	key := "default-ttl"
	setCachedToken(key, "Bearer tok", 0) // 0 should use default 60s

	tokenCacheMu.RLock()
	cached, ok := tokenCacheMap[key]
	tokenCacheMu.RUnlock()

	if !ok {
		t.Fatal("Expected token to be cached")
	}
	// Default TTL is 60s * 0.8 = 48s. Check it's in a reasonable range.
	remaining := time.Until(cached.expiresAt)
	if remaining < 40*time.Second || remaining > 50*time.Second {
		t.Errorf("Expected default TTL ~48s, got %v", remaining)
	}
}

func TestSetCachedTokenSafetyMargin(t *testing.T) {
	clearTokenCache()
	defer clearTokenCache()

	key := "margin-test"
	setCachedToken(key, "Bearer tok", 300) // 300s * 0.8 = 240s

	tokenCacheMu.RLock()
	cached, ok := tokenCacheMap[key]
	tokenCacheMu.RUnlock()

	if !ok {
		t.Fatal("Expected token to be cached")
	}
	remaining := time.Until(cached.expiresAt)
	// Should be ~240s, allow some slack
	if remaining < 235*time.Second || remaining > 245*time.Second {
		t.Errorf("Expected TTL ~240s (80%% of 300s), got %v", remaining)
	}
}

func TestSetCachedTokenOverwrite(t *testing.T) {
	clearTokenCache()
	defer clearTokenCache()

	key := "overwrite-test"
	setCachedToken(key, "Bearer old", 300)
	setCachedToken(key, "Bearer new", 600)

	token, ok := getCachedToken(key)
	if !ok {
		t.Error("Expected cache hit")
	}
	if token != "Bearer new" {
		t.Errorf("Expected overwritten token 'Bearer new', got %q", token)
	}
}

func TestCacheEmptyToken(t *testing.T) {
	clearTokenCache()
	defer clearTokenCache()

	// Empty token is valid (means "no auth needed")
	key := "no-auth"
	setCachedToken(key, "", 60)

	token, ok := getCachedToken(key)
	if !ok {
		t.Error("Expected cache hit for empty token")
	}
	if token != "" {
		t.Errorf("Expected empty token, got %q", token)
	}
}


