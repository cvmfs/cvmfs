package registry

import "testing"

func TestFilterUsingGlobAtBeginning(t *testing.T) {
	input := []string{"bar-foo", "ubuntu-foo", "foo-bar"}
	result, err := filterUsingGlob("*-foo", input)
	if err != nil {
		t.Errorf("Error in filtering: %s", err)
	}
	expected := []string{"bar-foo", "ubuntu-foo"}
	if len(result) != len(expected) {
		t.Errorf("The match is missing something, different lenghts %d != %d", len(result), len(input))
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
		t.Errorf("The match is missing something, different lenghts %d != %d", len(result), len(input))
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
		t.Errorf("The match is missing something, different lenghts %d != %d", len(result), len(input))
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
		t.Errorf("The match is missing something, different lenghts %d != %d", len(result), len(input))
	}
	for i := range result {
		if result[i] != expected[i] {
			t.Errorf("The match is missing something: %s != %s", result[i], input[i])
		}
	}
}
