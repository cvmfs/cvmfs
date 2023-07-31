package db

import (
	"reflect"
	"testing"
)

func TestParseWishInputURL(t *testing.T) {
	tests := []struct {
		name    string
		url     string
		want    ParsedWishInputURL
		wantErr bool
	}{
		{
			name: "test1",
			url:  "https://docker.io/library/alpine:latest",
			want: ParsedWishInputURL{Scheme: "https", Registry: "docker.io", Repository: "library/alpine", Tag: "latest", TagWildcard: false},
		},
		{
			name: "test2",
			url:  "https://docker.io/library/alpine@sha256:c0537ff6a5218ef531ece93d4984efc99bbf3f7497c0a7726c88e2bb7584dc96",
			want: ParsedWishInputURL{Scheme: "https", Registry: "docker.io", Repository: "library/alpine", Digest: "sha256:c0537ff6a5218ef531ece93d4984efc99bbf3f7497c0a7726c88e2bb7584dc96"},
		},
		{
			name: "test3",
			url:  "https://docker.io/library/alpine:latest@sha256:c0537ff6a5218ef531ece93d4984efc99bbf3f7497c0a7726c88e2bb7584dc96",
			want: ParsedWishInputURL{Scheme: "https", Registry: "docker.io", Repository: "library/alpine", Digest: "sha256:c0537ff6a5218ef531ece93d4984efc99bbf3f7497c0a7726c88e2bb7584dc96"},
		},
		{
			name:    "test4",
			url:     "badurl",
			wantErr: true,
		},
		{
			name:    "missingRepository",
			url:     "https://docker.io@sha256:c0537ff6a5218ef531ece93d4984efc99bbf3f7497c0a7726c88e2bb7584dc96",
			wantErr: true,
		},
		{
			name:    "invalidDigest",
			url:     "https://docker.io/library/alpine:latest@sha256:invalidDigest",
			wantErr: true,
		},
		{
			name: "wildcard",
			url:  "https://docker.io/library/alpine:*",
			want: ParsedWishInputURL{Scheme: "https", Registry: "docker.io", Repository: "library/alpine", TagWildcard: true, Tag: "*"},
		},
		{
			name: "wildcard2",
			url:  "https://docker.io/library/alpine:5.*",
			want: ParsedWishInputURL{Scheme: "https", Registry: "docker.io", Repository: "library/alpine", TagWildcard: true, Tag: "5.*"},
		},
		{
			// We want digest to take precedence over tag
			name: "tagAndDigest",
			url:  "https://docker.io/library/alpine:5.0@sha256:c0537ff6a5218ef531ece93d4984efc99bbf3f7497c0a7726c88e2bb7584dc96",
			want: ParsedWishInputURL{Scheme: "https", Registry: "docker.io", Repository: "library/alpine", Digest: "sha256:c0537ff6a5218ef531ece93d4984efc99bbf3f7497c0a7726c88e2bb7584dc96", TagWildcard: false, Tag: ""},
		},
		{
			// Since the digest takes precedence over the tag, we should not have a wildcard
			name: "tagAndDigestWithWildcard",
			url:  "https://docker.io/library/alpine:5.*@sha256:c0537ff6a5218ef531ece93d4984efc99bbf3f7497c0a7726c88e2bb7584dc96",
			want: ParsedWishInputURL{Scheme: "https", Registry: "docker.io", Repository: "library/alpine", Digest: "sha256:c0537ff6a5218ef531ece93d4984efc99bbf3f7497c0a7726c88e2bb7584dc96", TagWildcard: false, Tag: ""},
		},
		{
			// We default to latest if no tag is specified
			name: "defaultTag",
			url:  "https://docker.io/library/alpine",
			want: ParsedWishInputURL{Scheme: "https", Registry: "docker.io", Repository: "library/alpine", Tag: "latest"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseWishInputURL(tt.url)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseWishInputURL() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ParseWishInputURL() = %v, want %v", got, tt.want)
			}
		})
	}
}
