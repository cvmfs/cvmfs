package unpacker

import (
	"reflect"
	"testing"
)

func TestRemoveDuplicateStrings(t *testing.T) {
	tests := []struct {
		input []string
		want  []string
	}{
		{[]string{"a", "b", "b", "c"}, []string{"a", "b", "c"}},
		{[]string{"a"}, []string{"a"}},
		{[]string{}, []string{}},
		{[]string{"a", "a", "a", "a"}, []string{"a"}},
	}

	for _, tt := range tests {
		t.Run("ResultCheck", func(t *testing.T) {
			original := make([]string, len(tt.input))
			copy(original, tt.input)
			got := removeDuplicateStrings(tt.input)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("removeDuplicateStrings(%v) = %v; want %v", tt.input, got, tt.want)
			}
			if !reflect.DeepEqual(tt.input, original) {
				t.Errorf("removeDuplicateStrings altered the input. Got %v, wanted %v", tt.input, original)
			}
		})
	}
}

func TestIntersectionOfStringSets(t *testing.T) {
	tests := []struct {
		a, b []string
		want []string
	}{
		{[]string{"a", "b", "c"}, []string{"b", "c", "d"}, []string{"b", "c"}},
		{[]string{"a", "b"}, []string{"c", "d"}, []string{}},
		{[]string{}, []string{"a", "b"}, []string{}},
		{[]string{"a", "b"}, []string{}, []string{}},
		{[]string{"a", "a", "b", "c"}, []string{"a", "a", "c", "c"}, []string{"a", "c"}},
	}

	for _, tt := range tests {
		t.Run("ResultCheck", func(t *testing.T) {
			aOriginal := make([]string, len(tt.a))
			bOriginal := make([]string, len(tt.b))
			copy(aOriginal, tt.a)
			copy(bOriginal, tt.b)

			got := intersectionOfStringSets(tt.a, tt.b)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("intersectionOfStringSets(%v, %v) = %v; want %v", tt.a, tt.b, got, tt.want)
			}
			if !reflect.DeepEqual(tt.a, aOriginal) || !reflect.DeepEqual(tt.b, bOriginal) {
				t.Errorf("intersectionOfStringSets altered the inputs. Got %v and %v, wanted %v and %v", tt.a, tt.b, aOriginal, bOriginal)
			}
		})
	}
}

func TestDifferenceOfStringSets(t *testing.T) {
	tests := []struct {
		a, b []string
		want []string
	}{
		{[]string{"a", "b", "c"}, []string{"b", "c", "d"}, []string{"a"}},
		{[]string{"a", "b"}, []string{"c", "d"}, []string{"a", "b"}},
		{[]string{}, []string{"a", "b"}, []string{}},
		{[]string{"a", "b"}, []string{}, []string{"a", "b"}},
		{[]string{"a", "a", "b", "c"}, []string{"a", "a", "c", "c"}, []string{"b"}},
	}

	for _, tt := range tests {
		t.Run("ResultCheck", func(t *testing.T) {
			aOriginal := make([]string, len(tt.a))
			bOriginal := make([]string, len(tt.b))
			copy(aOriginal, tt.a)
			copy(bOriginal, tt.b)

			got := differenceOfStringSets(tt.a, tt.b)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("differenceOfStringSets(%v, %v) = %v; want %v", tt.a, tt.b, got, tt.want)
			}
			if !reflect.DeepEqual(tt.a, aOriginal) || !reflect.DeepEqual(tt.b, bOriginal) {
				t.Errorf("differenceOfStringSets altered the inputs. Got %v and %v, wanted %v and %v", tt.a, tt.b, aOriginal, bOriginal)
			}
		})
	}
}
