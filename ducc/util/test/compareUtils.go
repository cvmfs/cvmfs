package test

import (
	"fmt"
	"reflect"
)

func structIterator(s interface{}) {
	fields := reflect.VisibleFields(reflect.TypeOf(s))
	for _, field := range fields {
		fmt.Printf("Key: %s\tType: %s\n", field.Name, field.Type)
	}
}

func EqualsForFields(a interface{}, b interface{}, fields []string) bool {
	for _, field := range fields {
		_, okA := reflect.TypeOf(a).FieldByName(field)
		if !okA {
			fmt.Printf("EqualsForFields failed. Field \"%s\" not found in type \"%s\"\n", field, reflect.TypeOf(a))
			return false
		}
		_, okB := reflect.TypeOf(b).FieldByName(field)
		if !okB {
			fmt.Printf("EqualsForFields failed. Field \"%s\" not found in type \"%s\"\n", field, reflect.TypeOf(b))
			return false
		}

		fieldA := reflect.ValueOf(a).FieldByName(field).Interface()
		fieldB := reflect.ValueOf(b).FieldByName(field).Interface()

		if !reflect.DeepEqual(fieldA, fieldB) {
			fmt.Printf("EqualsForFields failed. Field \"%s\": \"%+v\" != \"%+v\"\n", field, fieldA, fieldB)
			return false
		}
	}
	return true
}

func EqualsExceptForFields(a interface{}, b interface{}, excemptFields []string) bool {
	all_fields := reflect.VisibleFields(reflect.TypeOf(a))
nextField:
	for _, field := range all_fields {
		// This is O(n^2), but it's only used for testing and a small number of fields.
		for _, exemptField := range excemptFields {
			if field.Name == exemptField {
				continue nextField
			}
		}
		fieldA := reflect.ValueOf(a).FieldByName(field.Name).Interface()
		fieldB := reflect.ValueOf(b).FieldByName(field.Name).Interface()
		if !(reflect.DeepEqual(fieldA, fieldB)) {
			fmt.Printf("EqualsExceptForFields failed. Field %s: %s != %s\n", field.Name, fieldA, fieldB)
			return false
		}
	}
	return true
}
